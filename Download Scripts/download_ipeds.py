"""
IPEDS "Power User" Data Downloader Script (v6 - Multithreaded, Comprehensive & Sorted)

PURPOSE:
This script automates the download and extraction of IPEDS "Complete Data Files"
for a specified range of years (2002-2024; 2001 is skipped because HD is absent).
It is designed to be run from a local
machine (e.g., in VS Code) to gather all necessary raw cross-sectional files
and their corresponding data dictionaries.

METHODOLOGY AND CITATION:
This script implements a methodology common in the academic research community
for handling IPEDS data. The core logic (programmatically querying the 
'DataFiles.aspx' page and parsing the HTML response) is a standard practice
to bypass the tedious manual download of hundreds of files.

While this specific script is generated from scratch, its methodology is 
informed by and similar to that found in community-supported tools like:
- The Urban Institute's 'ipeds-scraper' (Python)
- The 'ipedsr' package (R)
- Various 'StataIPEDSAll' scripts (Stata)

If you use this script in research, it is good practice to cite the IPEDS
data source itself (NCES). You can also note that data was "programmatically
downloaded using a custom Python script implementing established web-scraping
methodologies for the IPEDS Data Center."

CRITICAL NOTE ON ROBUSTNESS AND DATA INTEGRITY:
This script ONLY downloads the raw, cross-sectional files. It **DOES NOT**
perform any harmonization, cleaning, or paneling. 

The user is fully responsible for the critical research work of:
1.  **Harmonization:** Using the downloaded Data Dictionaries to map variable
    names that change over time (e.g., `F1C01` in one year vs. `F1D05` in another).
2.  **Crosswalking:** Reconciling changes in reporting standards (e.g.,
    GASB finance rules, 2010 and 2020 CIP code updates, race/ethnicity 
    category changes pre/post 2007).
3.  **De-duplicating:** Handling parent-child `UNITID` relationships.

This script's job is to give you all the raw materials. The "robustness" of
your final panel dataset depends on the analysis you perform *after*
using this script.
"""

import argparse
import csv
import hashlib
import os
import re
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from urllib.parse import parse_qs, urljoin, urlparse

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests
from bs4 import BeautifulSoup

DOWNLOAD_DIR = '/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data'
YEARS_TO_DOWNLOAD = range(2002, 2025)
BASE_URL = 'https://nces.ed.gov/ipeds/datacenter/'
USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/123.0.0.0 Safari/537.36'
)
HEADERS = {'User-Agent': USER_AGENT}
MAX_WORKERS = int(os.getenv('IPEDSDL_WORKERS', '3'))
DOWNLOAD_ACCESS_DATABASE = False
DICT_EXTENSION_PRIORITY = {
    '.zip': 3,
    '.xlsx': 2,
    '.xls': 1,
    '.csv': 1,
    '.txt': 1,
}
ALLOWED_DICT_EXTENSIONS = set(DICT_EXTENSION_PRIORITY.keys())
VARNAME_COLUMN_CANDIDATES = {
    'varname',
    'var_name',
    'variable',
    'var',
    'name',
    'column',
}
VARNAME_LABEL_COLUMN_CANDIDATES = {
    'label',
    'varlabel',
    'variable label',
    'variable_label',
    'vartitle',
    'var title',
    'description',
    'long description',
    'longdescription',
}
VARNAME_TABLE_COLUMN_CANDIDATES = {
    'table',
    'tablenm',
    'tab',
    'section',
    'worksheet',
    'sheet',
}
LOCAL_DICT_FILE_PRIORITY = ['.xlsx', '.xls', '.csv', '.txt']
VARNAME_SHEET_PRIORITY = ('varlist', 'variables', 'layout')
# This comprehensive map defines a "research name" (key) and
# all its known historical file prefixes (values).
SURVEY_DEFINITIONS: dict[str, list[str]] = {
    'Directory': ['HD'],
    'InstitutionalCharacteristics': ['IC'],
    'Completions': ['C'],
    'Derived': [
        'DRVADM',
        'DRVAL',
        'DRVC',
        'DRVEF12',
        'DRVEF',
        'DRVF',
        'DRVGR',
        'DRVHR',
        'DRVIC',
        'DRVOM',
    ],
    'FallEnrollment': ['EF', 'EFIA', 'EFIB', 'EFIC', 'EFID'],
    '12MonthEnrollment': ['E12', 'E1D'],
    'Finance': ['F'],
    'StudentFinancialAid': ['SFA'],
    'GraduationRates': ['GR', 'GRS', 'PE'],  # GRS/PE are historical
    'HumanResources': ['HR', 'S', 'SAL', 'EAP'],  # S, SAL, EAP are historical
    'OutcomeMeasures': ['OM'],
    'Admissions': ['ADM'],
    'AcademicLibraries': ['AL'],
    'Cost': ['VCOST'],
    'Flags': ['FLAGS'],
}

# Matches the uniquely named 12-Month Enrollment files (e.g., EFFY2004_RV.csv).
EFFY_SPECIAL_PATTERN = re.compile(r'EFFY[-_]?(\d{4})', re.IGNORECASE)
FINANCE_FORM_PATTERN = re.compile(r'(?:^|[_-])(F[123][A-Z0-9]+)')
HUMAN_RESOURCES_S_PATTERN = re.compile(r'(?:^|[_-])S(?:19|20)\d{2}')

SURVEY_ALIASES: dict[str, str] = {
    'DRV': 'Derived',
}


def ensure_directory(path: str) -> None:
    """Create a directory if it does not already exist."""
    os.makedirs(path, exist_ok=True)


def compute_file_metadata(path: str) -> tuple[str, str]:
    """Return (filesize_bytes, sha256) for the given path."""
    if not os.path.exists(path):
        return "", ""
    sha256 = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            size += len(chunk)
            sha256.update(chunk)
    return str(size), sha256.hexdigest()


def fetch_remote_filesize(session: requests.Session, url: str) -> str:
    """Attempt to retrieve the Content-Length without downloading the file."""
    try:
        response = session.head(url, allow_redirects=True, timeout=30, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException:
        return ""
    length = response.headers.get("Content-Length")
    if length and length.isdigit():
        return length
    return ""


def get_survey_prefixes_for_year(
    survey_name: str, survey_prefixes: list[str], year: int
) -> list[str]:
    """Return canonical filename prefixes for the survey.

    Filenames on the IPEDS site historically begin with a short survey code
    (e.g., ``E12`` or ``SFA``) followed by optional punctuation, academic-year
    tokens, or calendar-year suffixes.  Rather than try to enumerate every
    possible year-specific variation, which has proven brittle as the naming
    scheme evolves, we treat the base codes themselves as the matching keys and
    rely on longest-prefix ordering to disambiguate overlaps such as ``S`` vs
    ``SFA``.  To give slightly higher precedence to filenames that include an
    underscore or hyphen immediately after the code, we include those variants
    as explicit entries as well.
    """

    configured_prefixes = survey_prefixes or [survey_name]

    prefixes: set[str] = set()
    for prefix in configured_prefixes:
        prefix_upper = prefix.upper()
        prefixes.add(prefix_upper)
        prefixes.add(f"{prefix_upper}_")
        prefixes.add(f"{prefix_upper}-")

    return sorted(prefixes, key=len, reverse=True)


def build_prefix_pattern(prefix: str) -> re.Pattern[str]:
    """Return a regex that matches the prefix at token boundaries."""
    prefix_core = prefix.rstrip('_-') or prefix
    boundary = r'(?:^|[_-])'
    if prefix_core == 'C':
        trailing = r'(?=\d{4})'
    else:
        trailing = r'(?=[A-Z0-9])'
    return re.compile(rf'{boundary}{re.escape(prefix_core)}{trailing}')


def fetch_year_page(session: requests.Session, year: int) -> BeautifulSoup | None:
    """Retrieve and parse the HTML page listing files for a given year."""
    url = urljoin(BASE_URL, f'DataFiles.aspx?year={year}&surveyNumber=-1')
    try:
        response = session.get(url, timeout=60, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"ERROR: Unable to fetch file list for {year}: {exc}")
        return None
    return BeautifulSoup(response.text, 'html.parser')


def parse_year_links(
    soup: BeautifulSoup, year: int
) -> tuple[dict[str, dict[str, dict[str, list[dict]]]], list[dict]]:
    """Parse the year's HTML and choose the data/dictionary links plus Access DB."""
    found_link = False

    prefix_map: dict[str, tuple[str, str]] = {}
    for survey_name, survey_prefixes in SURVEY_DEFINITIONS.items():
        for prefix in get_survey_prefixes_for_year(survey_name, survey_prefixes, year):
            base_prefix = prefix.rstrip('_-') or prefix
            prefix_map[prefix] = (survey_name, base_prefix)

    prefixes = list(prefix_map.keys())
    # Sort by length so that longer, more specific prefixes (e.g., SFA2004)
    # are evaluated before shorter ones that could otherwise capture the same
    # file (e.g., S2004).
    prefixes.sort(key=len, reverse=True)

    prefix_patterns: dict[str, re.Pattern[str]] = {
        prefix: build_prefix_pattern(prefix) for prefix in prefixes
    }

    def refine_matched_prefix(
        survey_name: str, filename_upper: str, base_prefix: str
    ) -> str:
        """Return a sub-prefix token when available (e.g., F1A vs F2A)."""
        if survey_name == 'Finance':
            form_match = FINANCE_FORM_PATTERN.search(filename_upper)
            if form_match:
                return form_match.group(1)
        return base_prefix

    def is_valid_generic_prefix(
        survey_name: str, base_prefix: str, filename_upper: str
    ) -> bool:
        """Filter overly broad prefixes such as 'S' and 'C'."""
        if survey_name == 'HumanResources' and base_prefix == 'S':
            return bool(HUMAN_RESOURCES_S_PATTERN.search(filename_upper))
        return True

    def identify_survey(filename_upper: str) -> tuple[str, str] | None:
        """Return the matching survey and refined prefix (if applicable)."""
        special_match = EFFY_SPECIAL_PATTERN.search(filename_upper)
        if special_match and special_match.group(1) == str(year):
            return '12MonthEnrollment', 'EFFY'

        for prefix in prefixes:
            if prefix_patterns[prefix].search(filename_upper):
                survey_name, base_prefix = prefix_map[prefix]
                if not is_valid_generic_prefix(survey_name, base_prefix, filename_upper):
                    continue
                refined = refine_matched_prefix(survey_name, filename_upper, base_prefix)
                return survey_name, refined

        return None

    survey_results: defaultdict[
        str, defaultdict[str, dict[str, dict[str, dict]]]
    ] = defaultdict(lambda: defaultdict(lambda: {'_best_data': {}, '_best_dict': {}}))
    access_entries: list[dict] = []

    for row_idx, row in enumerate(soup.find_all('tr')):
        row_text_lower = (row.get_text(separator=' ', strip=True) or '').lower()
        for link in row.find_all('a', href=True):
            row_id = f'row-{row_idx}'
            link_text = (link.get_text() or '').strip().lower()
            href = link['href']
            full_url = urljoin(BASE_URL, href)
            parsed = urlparse(full_url)
            path_lower = parsed.path.lower()
            query_params = parse_qs(parsed.query)

            entry_type = None
            filename = ""
            filename_for_match = ""
            is_revision = False
            ext_priority = 0

            if 'data-generator' in path_lower:
                table_name = (
                    query_params.get('tableName')
                    or query_params.get('tablename')
                    or ['']
                )[0].strip()
                if not table_name:
                    continue
                file_type = (query_params.get('type') or ['csv'])[0].lower()
                if file_type and file_type != 'csv':
                    # Only pull the CSV export to avoid duplicate Stata downloads.
                    continue
                has_rv = (query_params.get('hasrv') or ['0'])[0].lower()
                is_revision = has_rv not in {'0', 'false', ''}
                entry_type = 'data'
                filename = f"{table_name}.zip"
                filename_for_match = table_name.upper()
                ext_priority = 1
            elif 'dictionary-generator' in path_lower:
                table_name = (
                    query_params.get('tableName')
                    or query_params.get('tablename')
                    or ['']
                )[0].strip()
                if not table_name:
                    continue
                entry_type = 'dict'
                filename = f"{table_name}_Dict.zip"
                filename_for_match = table_name.upper()
                ext_priority = DICT_EXTENSION_PRIORITY.get('.zip', 0)
            elif '/ipeds/datacenter/data/' in full_url.lower():
                if 'access' in link_text and 'database' in link_text:
                    filename = os.path.basename(parsed.path)
                    if not filename:
                        continue
                    is_revision = '_RV' in filename.upper()
                    ext = os.path.splitext(filename)[1].lower()
                    ext_priority = 1 if ext == '.zip' else 0
                    access_entries.append(
                        {
                            'priority': (1 if is_revision else 0, ext_priority),
                            'url': full_url,
                            'filename': filename,
                            'is_revision': is_revision,
                            'release': 'revised' if is_revision else '',
                        }
                    )
                    continue

                filename = os.path.basename(parsed.path)
                if not filename:
                    continue
                filename_for_match = filename.upper()
                entry_type = (
                    'dict'
                    if ('_DICT' in filename_for_match or 'dictionary' in link_text)
                    else 'data'
                )
                is_revision = '_RV' in filename_for_match
                ext = os.path.splitext(filename)[1].lower()
                if entry_type == 'dict':
                    if ext and ALLOWED_DICT_EXTENSIONS and ext not in ALLOWED_DICT_EXTENSIONS:
                        continue
                    ext_priority = DICT_EXTENSION_PRIORITY.get(ext, 0)
                else:
                    ext_priority = 1 if ext == '.zip' else 0
            else:
                continue

            survey_match = identify_survey(filename_for_match)
            if survey_match is None:
                continue

            found_link = True
            survey, matched_prefix = survey_match
            revision_priority = 1 if is_revision else 0

            release = 'revised' if 'revised' in row_text_lower else ''
            if not release and 'provisional' in row_text_lower:
                release = 'provisional'
            if not release and is_revision:
                release = 'revised'

            candidate = {
                'priority': (revision_priority, ext_priority),
                'url': full_url,
                'filename': filename,
                'is_revision': is_revision,
                'row_id': row_id,
                'release': release,
            }

            bucket = survey_results[survey][matched_prefix]
            best_key = '_best_dict' if entry_type == 'dict' else '_best_data'
            best_by_row = bucket[best_key]
            existing = best_by_row.get(row_id)
            if (existing is None) or (candidate['priority'] > existing['priority']):
                best_by_row[row_id] = candidate

    if not found_link:
        print(f"WARNING: No download links found for {year}.")
        return {}, []

    final_results: dict[str, dict[str, dict[str, list[dict]]]] = {}
    for survey, prefix_map in survey_results.items():
        final_results[survey] = {}
        for prefix, entries in prefix_map.items():
            data_candidates = sorted(
                entries.get('_best_data', {}).values(),
                key=lambda entry: entry['priority'],
                reverse=True,
            )
            dict_candidates = sorted(
                entries.get('_best_dict', {}).values(),
                key=lambda entry: entry['priority'],
                reverse=True,
            )
            final_results[survey][prefix] = {
                'data': data_candidates,
                'dict': dict_candidates,
            }

    return final_results, access_entries


def download_file(
    session: requests.Session,
    url: str,
    destination: str,
    *,
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
) -> bool:
    """Download a file from the provided URL to the destination path."""
    attempt = 1
    delay = backoff_seconds
    while attempt <= max_attempts:
        try:
            with session.get(url, stream=True, timeout=120, headers=HEADERS) as response:
                response.raise_for_status()
                content_type = (response.headers.get("Content-Type") or "").lower()
                dest_ext = os.path.splitext(destination)[1].lower()
                if "text/html" in content_type and dest_ext not in {".html", ".htm"}:
                    preview = ""
                    try:
                        preview = response.content[:512].decode("utf-8", errors="replace")
                    except Exception:  # noqa: BLE001
                        preview = ""
                    raise ValueError(
                        f"HTML directory page returned instead of file. URL: {url}\n"
                        f"{preview[:200].strip()}"
                    )
                with open(destination, 'wb') as file_obj:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            file_obj.write(chunk)
            return True
        except ValueError as exc:
            print(f"WARNING: {exc}")
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else None
            retriable = status_code in {403, 429, 500, 502, 503, 504}
            if not retriable or attempt == max_attempts:
                print(f"ERROR: Failed to download {url}: {exc}")
                break
        except requests.RequestException as exc:
            if attempt == max_attempts:
                print(f"ERROR: Failed to download {url}: {exc}")
                break
        except OSError as exc:
            print(f"ERROR: Unable to write file {destination}: {exc}")
            break

        print(f"Retrying {url} in {delay:.1f}s (attempt {attempt}/{max_attempts})...")
        time.sleep(delay)
        attempt += 1
        delay *= 2

    return False


def unzip_and_remove(zip_path: str, extract_to: str, *, context: str = '') -> None:
    """Extract a zip file to the target directory and delete the original archive."""
    base_name = os.path.splitext(os.path.basename(zip_path))[0]
    target_dir = os.path.join(extract_to, base_name)
    try:
        os.makedirs(target_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as archive:
            archive.extractall(target_dir)
        os.remove(zip_path)
        extracted_items = os.listdir(target_dir)
        if not extracted_items:
            print(
                f"WARNING: No files extracted from {context or os.path.basename(zip_path)} "
                f"into {os.path.relpath(target_dir, extract_to)}"
            )
        print(
            f"Unzipped {os.path.basename(zip_path)} into "
            f"{os.path.relpath(target_dir, extract_to)} and removed archive"
        )
    except zipfile.BadZipFile:
        print(f"WARNING: {os.path.basename(zip_path)} is not a valid zip file.")
    except OSError as exc:
        print(f"ERROR: Unable to process {zip_path}: {exc}")


def _normalize_colname(name: object) -> str:
    return str(name).strip().lower() if name is not None else ""


def _pick_column(columns: list, candidates: set[str]) -> str | None:
    for col in columns:
        if _normalize_colname(col) in candidates:
            return col
    return None


def _sheet_priority(name: str) -> tuple[int, str]:
    lowered = name.lower()
    for idx, hint in enumerate(VARNAME_SHEET_PRIORITY):
        if hint in lowered:
            return idx, lowered
    return len(VARNAME_SHEET_PRIORITY), lowered


def _extract_varnames_from_dataframe(
    df, *, sheet_name: str | None, pd_mod
) -> list[dict[str, str]]:
    var_col = _pick_column(list(df.columns), VARNAME_COLUMN_CANDIDATES)
    if not var_col:
        return []
    label_col = _pick_column(list(df.columns), VARNAME_LABEL_COLUMN_CANDIDATES)
    table_col = _pick_column(list(df.columns), VARNAME_TABLE_COLUMN_CANDIDATES)

    records: list[dict[str, str]] = []
    for _, row in df.iterrows():
        var_val = row[var_col]
        if pd_mod.isna(var_val):
            continue
        varname = str(var_val).strip()
        if not varname or varname.lower() == "nan":
            continue
        record = {
            'varname': varname,
            'label': '',
            'table': '',
            'sheet': sheet_name or '',
        }
        if label_col is not None:
            label_val = row[label_col]
            if not pd_mod.isna(label_val):
                record['label'] = str(label_val).strip()
        if table_col is not None:
            table_val = row[table_col]
            if not pd_mod.isna(table_val):
                record['table'] = str(table_val).strip()
        records.append(record)
    return records


def extract_varnames_from_file(path: str) -> list[dict[str, str]]:
    """Return varName rows (varname/label/table/sheet) from a dictionary file."""
    try:
        import pandas as pd
    except ImportError:
        print("WARNING: pandas is required for --extract-varnames; skipping extraction.")
        return []

    ext = os.path.splitext(path)[1].lower()
    if ext in {'.xlsx', '.xls'}:
        try:
            excel = pd.ExcelFile(path)
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING: Unable to read {os.path.basename(path)}: {exc}")
            return []
        sheet_names = sorted(excel.sheet_names, key=_sheet_priority)
        rows: list[dict[str, str]] = []
        for sheet in sheet_names:
            try:
                df = excel.parse(sheet)
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: Unable to parse sheet {sheet} in {os.path.basename(path)}: {exc}")
                continue
            sheet_rows = _extract_varnames_from_dataframe(df, sheet_name=sheet, pd_mod=pd)
            if sheet_rows:
                for row in sheet_rows:
                    row['sheet'] = sheet
                rows.extend(sheet_rows)
        return rows

    if ext in {'.htm', '.html'}:
        try:
            tables = pd.read_html(path)
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING: Unable to read HTML dictionary {os.path.basename(path)}: {exc}")
            return []
        rows: list[dict[str, str]] = []
        for idx, df in enumerate(tables):
            sheet_rows = _extract_varnames_from_dataframe(df, sheet_name=f"html_table_{idx}", pd_mod=pd)
            rows.extend(sheet_rows)
        return rows

    try:
        df = pd.read_csv(path, engine='python')
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: Unable to read dictionary file {os.path.basename(path)}: {exc}")
        return []
    return _extract_varnames_from_dataframe(df, sheet_name=None, pd_mod=pd)


def extract_varnames_for_dictionary(
    dict_destination: str, year_dir: str
) -> list[dict[str, str]]:
    """Find and extract varName rows from a downloaded dictionary."""
    candidates: list[str] = []
    if dict_destination.lower().endswith('.zip'):
        extracted_dir = os.path.join(year_dir, os.path.splitext(os.path.basename(dict_destination))[0])
        if os.path.isdir(extracted_dir):
            for root, _dirs, files in os.walk(extracted_dir):
                for name in files:
                    candidates.append(os.path.join(root, name))
    if os.path.isfile(dict_destination):
        candidates.append(dict_destination)

    def priority(path: str) -> int:
        ext = os.path.splitext(path)[1].lower()
        try:
            return LOCAL_DICT_FILE_PRIORITY.index(ext)
        except ValueError:
            return len(LOCAL_DICT_FILE_PRIORITY)

    candidates = [path for path in candidates if os.path.splitext(path)[1].lower() in LOCAL_DICT_FILE_PRIORITY]
    candidates.sort(key=priority)

    combined: list[dict[str, str]] = []
    seen: set[str] = set()
    for path in candidates:
        rows = extract_varnames_from_file(path)
        if not rows:
            continue
        for row in rows:
            row['source_file'] = os.path.basename(path)
            key = (row.get('varname') or '').strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            combined.append(row)
    return combined


def find_dictionary_for_data(
    dict_entries: list[dict], data_entry: dict
) -> dict | None:
    """Select the dictionary that shares a row_id with the data entry."""
    if not dict_entries:
        return None

    same_row = [entry for entry in dict_entries if entry.get('row_id') == data_entry.get('row_id')]
    candidates = same_row or dict_entries
    candidates.sort(key=lambda entry: entry['priority'], reverse=True)
    return candidates[0]


def prepare_entries(entries: list[dict]) -> list[dict]:
    """Return entries sorted by priority (revision + extension)."""
    return sorted(entries, key=lambda entry: entry['priority'], reverse=True)


def download_access_database(
    session: requests.Session, year: int, year_dir: str, access_entries: list[dict]
) -> None:
    """Download the Access database for the year if the option is enabled."""
    if not access_entries:
        print(f"WARNING: Access database not found for {year}.")
        return

    best_entry = max(access_entries, key=lambda entry: entry['priority'])
    destination = os.path.join(year_dir, best_entry['filename'])
    print(f"Downloading Access database {best_entry['filename']}...")
    if download_file(session, best_entry['url'], destination):
        if destination.lower().endswith('.zip'):
            print(f"Unzipping {best_entry['filename']}...")
            unzip_and_remove(destination, year_dir, context=best_entry['filename'])
        time.sleep(1)


def write_year_manifest(year_dir: str, year: int, rows: list[dict]) -> None:
    """Persist a manifest describing the year's downloaded files."""
    if not rows:
        return
    manifest_path = os.path.join(year_dir, f'{year}_manifest.csv')
    fieldnames = [
        'year',
        'survey',
        'prefix',
        'filename',
        'url',
        'is_revision',
        'has_dictionary',
        'dictionary_filename',
        'release',
        'filesize_bytes',
        'sha256',
    ]
    try:
        with open(manifest_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote manifest with {len(rows)} rows to {manifest_path}")
    except OSError as exc:
        print(f"WARNING: Unable to write manifest for {year}: {exc}")


def write_year_varnames(year_dir: str, year: int, rows: list[dict]) -> None:
    """Persist a per-year catalog of varNames pulled from dictionaries."""
    if not rows:
        return
    out_path = os.path.join(year_dir, f'{year}_varnames.csv')
    fieldnames = [
        'year',
        'survey',
        'prefix',
        'dictionary',
        'source_file',
        'sheet',
        'varname',
        'label',
        'table',
    ]
    try:
        with open(out_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote varName catalog with {len(rows)} rows to {out_path}")
    except OSError as exc:
        print(f"WARNING: Unable to write varName catalog for {year}: {exc}")


def process_year(
    year: int,
    *,
    manifest_only: bool = False,
    allowed_surveys: set[str] | None = None,
    extract_varnames: bool = False,
) -> None:
    """Process downloads for a single year, handling all configured surveys."""
    print(f"\n>>> Processing Year {year}...")
    with requests.Session() as session:
        session.headers.update(HEADERS)
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=['GET'],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_maxsize=MAX_WORKERS)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        soup = fetch_year_page(session, year)
        if soup is None:
            return

        survey_links, access_entries = parse_year_links(soup, year)
        try:
            soup.decompose()
        except Exception:
            pass
        del soup

        year_dir = os.path.join(DOWNLOAD_DIR, str(year))
        ensure_directory(year_dir)
        year_manifest: list[dict] = []
        year_varnames: list[dict] = []

        for survey_name in SURVEY_DEFINITIONS.keys():
            if allowed_surveys is not None and survey_name not in allowed_surveys:
                continue
            prefix_groups = survey_links.get(survey_name, {})

            if not prefix_groups:
                print(f"WARNING: Data files for survey {survey_name} not found for {year}.")
                continue

            for prefix_label, entry_group in prefix_groups.items():
                data_entries = prepare_entries(entry_group.get('data', []))
                dict_entries = prepare_entries(entry_group.get('dict', []))

                if not data_entries:
                    print(
                        f"WARNING: Data file for survey {survey_name} ({prefix_label}) "
                        f"not found for {year}."
                    )
                    continue

                if not dict_entries:
                    print(
                        f"WARNING: Dictionary for survey {survey_name} ({prefix_label}) "
                        f"not found for {year}."
                    )

                downloaded_dicts: set[str] = set()
                for data_entry in data_entries:
                    filename = data_entry['filename']
                    file_size = ""
                    file_hash = ""
                    if manifest_only:
                        file_size = fetch_remote_filesize(session, data_entry['url'])
                    else:
                        if data_entry['is_revision']:
                            print(
                                f"Downloading {filename} for {survey_name} ({prefix_label}) "
                                "(Prioritizing revised file)"
                            )
                        else:
                            print(f"Downloading {filename} for {survey_name} ({prefix_label})...")
                        destination = os.path.join(year_dir, filename)
                        if download_file(session, data_entry['url'], destination):
                            file_size, file_hash = compute_file_metadata(destination)
                            if destination.lower().endswith('.zip'):
                                print(f"Unzipping {filename}...")
                                unzip_and_remove(destination, year_dir, context=filename)
                            time.sleep(1)

                    dict_entry = find_dictionary_for_data(dict_entries, data_entry)
                    manifest_record = {
                        'year': year,
                        'survey': survey_name,
                        'prefix': prefix_label,
                        'filename': filename,
                        'url': data_entry['url'],
                        'is_revision': data_entry['is_revision'],
                        'has_dictionary': bool(dict_entry),
                        'dictionary_filename': dict_entry['filename'] if dict_entry else '',
                        'release': data_entry.get('release', ''),
                        'filesize_bytes': file_size,
                        'sha256': file_hash,
                    }
                    year_manifest.append(manifest_record)

                    if dict_entry is None:
                        print(
                            f"WARNING: Matching dictionary not found for "
                            f"{survey_name} ({prefix_label}) file {filename}."
                        )
                        continue

                    if manifest_only:
                        continue

                    dict_filename = dict_entry['filename']
                    if dict_filename in downloaded_dicts:
                        continue

                    if dict_entry['is_revision']:
                        print(
                            f"Downloading {dict_filename} for {survey_name} ({prefix_label}) "
                            "(Prioritizing revised file)"
                        )
                    else:
                        print(
                            f"Downloading {dict_filename} for {survey_name} ({prefix_label})..."
                        )
                    dict_destination = os.path.join(year_dir, dict_filename)
                    if download_file(session, dict_entry['url'], dict_destination):
                        if dict_destination.lower().endswith('.zip'):
                            print(f"Unzipping {dict_filename}...")
                            unzip_and_remove(dict_destination, year_dir, context=dict_filename)
                        if extract_varnames:
                            var_rows = extract_varnames_for_dictionary(dict_destination, year_dir)
                            if var_rows:
                                for row in var_rows:
                                    row.update(
                                        {
                                            'year': year,
                                            'survey': survey_name,
                                            'prefix': prefix_label,
                                            'dictionary': dict_filename,
                                        }
                                    )
                                year_varnames.extend(var_rows)
                            else:
                                print(
                                    f"WARNING: Unable to extract varName entries from {dict_filename} "
                                    f"for {survey_name} ({prefix_label})."
                                )
                        time.sleep(1)
                    downloaded_dicts.add(dict_filename)

        if DOWNLOAD_ACCESS_DATABASE and not manifest_only:
            download_access_database(session, year, year_dir, access_entries)

        if extract_varnames and year_varnames:
            write_year_varnames(year_dir, year, year_varnames)

        write_year_manifest(year_dir, year, year_manifest)


def _parse_years(expr: str | None) -> list[int]:
    if not expr:
        return list(YEARS_TO_DOWNLOAD)
    text = expr.strip()
    if not text:
        return list(YEARS_TO_DOWNLOAD)
    if ":" in text:
        start, end = text.split(":", 1)
        start, end = int(start), int(end)
        if end < start:
            start, end = end, start
        return list(range(start, end + 1))
    if "," in text:
        years: list[int] = []
        for part in text.split(","):
            part = part.strip()
            if part:
                years.append(int(part))
        return years
    return [int(text)]


def _parse_surveys(expr: str | None) -> set[str] | None:
    if expr is None:
        return None
    text = expr.strip()
    if not text:
        return None
    allowed: set[str] = set()
    valid_map = {name.lower(): name for name in SURVEY_DEFINITIONS}
    alias_map = {alias.lower(): target.lower() for alias, target in SURVEY_ALIASES.items()}
    for part in text.split(","):
        key = part.strip().lower()
        if not key:
            continue
        target = valid_map.get(key) or valid_map.get(alias_map.get(key, ""))
        if not target:
            valid_names = ", ".join(SURVEY_DEFINITIONS.keys())
            raise ValueError(f"Unknown survey '{part}'. Valid options: {valid_names}")
        allowed.add(target)
    return allowed or None


def main(argv: list[str] | None = None) -> None:
    global DOWNLOAD_DIR  # noqa: PLW0603
    parser = argparse.ArgumentParser(description="Download IPEDS raw data files and dictionaries.")
    parser.add_argument(
        "--out-root",
        default=DOWNLOAD_DIR,
        help="Root folder where year subdirectories are written",
    )
    parser.add_argument(
        "--years",
        default="",
        help="Year expression: 'YYYY', 'YYYY:YYYY', or comma list 'YYYY,YYYY'",
    )
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="Skip downloading files; only emit manifests (fetches Content-Length when possible).",
    )
    parser.add_argument(
        "--extract-varnames",
        action="store_true",
        help="Pull varName + labels from downloaded dictionaries into a {year}_varnames.csv file.",
    )
    parser.add_argument(
        "--surveys",
        default="",
        help="Comma list of surveys to download (matches SURVEY_DEFINITIONS keys; alias: DRV=Derived).",
    )
    args = parser.parse_args(argv)
    DOWNLOAD_DIR = args.out_root
    years = _parse_years(args.years)
    allowed_surveys = _parse_surveys(args.surveys)
    if args.extract_varnames and args.manifest_only:
        print("WARNING: --extract-varnames is ignored when --manifest-only is set (no dictionaries downloaded).")

    ensure_directory(DOWNLOAD_DIR)
    worker = partial(
        process_year,
        manifest_only=args.manifest_only,
        allowed_surveys=allowed_surveys,
        extract_varnames=args.extract_varnames,
    )
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(executor.map(worker, years))


if __name__ == '__main__':
    main()
