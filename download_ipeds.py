"""
IPEDS "Power User" Data Downloader Script (v6 - Multithreaded, Comprehensive & Sorted)

PURPOSE:
This script automates the download and extraction of IPEDS "Complete Data Files"
for a specified range of years (2004-2024). It is designed to be run from a local
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

import os
import re
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

DOWNLOAD_DIR = '/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas'
YEARS_TO_DOWNLOAD = range(2004, 2025)
BASE_URL = 'https://nces.ed.gov/ipeds/datacenter/'
USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/123.0.0.0 Safari/537.36'
)
HEADERS = {'User-Agent': USER_AGENT}
MAX_WORKERS = 5
DOWNLOAD_ACCESS_DATABASE = False
DICT_EXTENSION_PRIORITY = {'.zip': 2, '.xlsx': 1, '.xls': 0}
# This comprehensive map defines a "research name" (key) and
# all its known historical file prefixes (values).
SURVEY_DEFINITIONS: dict[str, list[str]] = {
    'Directory': ['HD'],
    'InstitutionalCharacteristics': ['IC'],
    'Completions': ['C'],
    'FallEnrollment': ['EF', 'EFIA', 'EFIB', 'EFIC', 'EFID'],
    '12MonthEnrollment': ['E12', 'E1D'],
    'Finance': ['F'],
    'StudentFinancialAid': ['SFA'],
    'GraduationRates': ['GR', 'GRS', 'PE'],  # GRS/PE are historical
    'HumanResources': ['HR', 'S', 'SAL', 'EAP'],  # S, SAL, EAP are historical
    'OutcomeMeasures': ['OM'],
    'Admissions': ['ADM'],
    'AcademicLibraries': ['AL'],
}

# Matches the uniquely named 12-Month Enrollment files (e.g., EFFY2004_RV.csv).
EFFY_SPECIAL_PATTERN = re.compile(r'EFFY[-_]?(\d{4})', re.IGNORECASE)
FINANCE_FORM_PATTERN = re.compile(r'(?:^|[_-])(F[123][A-Z0-9]+)')


def ensure_directory(path: str) -> None:
    """Create a directory if it does not already exist."""
    os.makedirs(path, exist_ok=True)


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


def fetch_year_page(session: requests.Session, year: int) -> BeautifulSoup | None:
    """Retrieve and parse the HTML page listing files for a given year."""
    url = urljoin(BASE_URL, f'DataFiles.aspx?year={year}')
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
    links = soup.find_all('a', href=True)
    if not links:
        print(f"WARNING: No download links found for {year}.")
        return {}, []

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
        prefix: re.compile(rf'(?:^|[^A-Z0-9]){re.escape(prefix)}') for prefix in prefixes
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

    def identify_survey(filename_upper: str) -> tuple[str, str] | None:
        """Return the matching survey and refined prefix (if applicable)."""
        for prefix in prefixes:
            if prefix_patterns[prefix].search(filename_upper):
                survey_name, base_prefix = prefix_map[prefix]
                refined = refine_matched_prefix(survey_name, filename_upper, base_prefix)
                return survey_name, refined

        special_match = EFFY_SPECIAL_PATTERN.search(filename_upper)
        if special_match and special_match.group(1) == str(year):
            return '12MonthEnrollment', 'EFFY'
        return None

    survey_results: defaultdict[
        str, defaultdict[str, dict[str, list[dict]]]
    ] = defaultdict(lambda: defaultdict(lambda: {'data': [], 'dict': []}))
    access_entries: list[dict] = []

    for row_idx, link in [
        (idx, anchor)
        for idx, row in enumerate(soup.find_all('tr'))
        for anchor in row.find_all('a', href=True)
    ]:
        row_id = f'row-{row_idx}'
        link_text = (link.get_text() or '').strip().lower()
        href = link['href']
        full_url = urljoin(BASE_URL, href)
        if '/ipeds/datacenter/data/' not in full_url.lower():
            continue
        if 'access' in link_text and 'database' in link_text:
            parsed = urlparse(full_url)
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
                }
            )
            continue
        parsed = urlparse(full_url)
        filename = os.path.basename(parsed.path)
        if not filename:
            continue

        filename_upper = filename.upper()

        survey_match = identify_survey(filename_upper)
        if survey_match is None:
            continue

        survey, matched_prefix = survey_match
        entry_type = (
            'dict' if ('_DICT' in filename_upper or 'dictionary' in link_text) else 'data'
        )
        is_revision = '_RV' in filename_upper
        revision_priority = 1 if is_revision else 0
        ext = os.path.splitext(filename)[1].lower()
        if entry_type == 'dict':
            ext_priority = DICT_EXTENSION_PRIORITY.get(ext, 0)
        else:
            ext_priority = 1 if ext == '.zip' else 0

        candidate = {
            'priority': (revision_priority, ext_priority),
            'url': full_url,
            'filename': filename,
            'is_revision': is_revision,
            'row_id': row_id,
        }

        survey_results[survey][matched_prefix][entry_type].append(candidate)

    # Convert defaultdicts into standard dicts for downstream processing
    final_results: dict[str, dict[str, dict[str, list[dict]]]] = {}
    for survey, prefix_map in survey_results.items():
        final_results[survey] = {prefix: entries for prefix, entries in prefix_map.items()}

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
            with session.get(
                url,
                stream=True,
                timeout=120,
                headers=HEADERS,
            ) as response:
                response.raise_for_status()
                with open(destination, 'wb') as file_obj:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file_obj.write(chunk)
            return True
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else None
            retriable = status_code in {403, 429, 503}
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


def unzip_and_remove(zip_path: str, extract_to: str) -> None:
    """Extract a zip file to the target directory and delete the original archive."""
    base_name = os.path.splitext(os.path.basename(zip_path))[0]
    target_dir = os.path.join(extract_to, base_name)
    try:
        os.makedirs(target_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as archive:
            archive.extractall(target_dir)
        os.remove(zip_path)
        print(
            f"Unzipped {os.path.basename(zip_path)} into "
            f"{os.path.relpath(target_dir, extract_to)} and removed archive"
        )
    except zipfile.BadZipFile:
        print(f"WARNING: {os.path.basename(zip_path)} is not a valid zip file.")
    except OSError as exc:
        print(f"ERROR: Unable to process {zip_path}: {exc}")


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
            unzip_and_remove(destination, year_dir)
        time.sleep(1)

def process_year(year: int) -> None:
    """Process downloads for a single year, handling all configured surveys."""
    print(f"\n>>> Processing Year {year}...")
    with requests.Session() as session:
        session.headers.update(HEADERS)
        soup = fetch_year_page(session, year)
        if soup is None:
            return

        survey_links, access_entries = parse_year_links(soup, year)
        year_dir = os.path.join(DOWNLOAD_DIR, str(year))
        ensure_directory(year_dir)

        for survey_name in SURVEY_DEFINITIONS.keys():
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
                    if data_entry['is_revision']:
                        print(
                            f"Downloading {filename} for {survey_name} ({prefix_label}) "
                            "(Prioritizing revised file)"
                        )
                    else:
                        print(f"Downloading {filename} for {survey_name} ({prefix_label})...")
                    destination = os.path.join(year_dir, filename)
                    if download_file(session, data_entry['url'], destination):
                        if destination.lower().endswith('.zip'):
                            print(f"Unzipping {filename}...")
                            unzip_and_remove(destination, year_dir)
                        time.sleep(1)

                    dict_entry = find_dictionary_for_data(dict_entries, data_entry)
                    if dict_entry is None:
                        print(
                            f"WARNING: Matching dictionary not found for "
                            f"{survey_name} ({prefix_label}) file {filename}."
                        )
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
                            unzip_and_remove(dict_destination, year_dir)
                        time.sleep(1)
                    downloaded_dicts.add(dict_filename)

        if DOWNLOAD_ACCESS_DATABASE:
            download_access_database(session, year, year_dir, access_entries)


def main() -> None:
    ensure_directory(DOWNLOAD_DIR)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(executor.map(process_year, YEARS_TO_DOWNLOAD))


if __name__ == '__main__':
    main()
