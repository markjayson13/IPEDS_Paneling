"""
IPEDS "Power User" Data Downloader Script (v4 - Robust Prefix Sort)

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
import time
import zipfile
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

DOWNLOAD_DIR = '/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas'
YEARS_TO_DOWNLOAD = range(2004, 2025)
SURVEYS_TO_DOWNLOAD = ['HD', 'IC', 'EF', 'F', 'C', 'GR', 'SFA', 'OM', 'HR', 'ADM', 'E12']
BASE_URL = 'https://nces.ed.gov/ipeds/datacenter/'
USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/123.0.0.0 Safari/537.36'
)
HEADERS = {'User-Agent': USER_AGENT}
DICT_EXTENSION_PRIORITY = {'.zip': 2, '.xlsx': 1, '.xls': 0}

HISTORICAL_SURVEY_MAP: dict[str, list[str]] = {
    'HD': ['HD'],
    'IC': ['IC'],
    'EF': ['EF'],
    'F': ['F'],
    'C': ['C'],
    'GR': ['GR'],
    'SFA': ['SFA'],
    'OM': ['OM'],
    'HR': ['HR', 'S', 'SAL', 'EAP'],
    'ADM': ['ADM'],
    'E12': ['E12'],
}


def ensure_directory(path: str) -> None:
    """Create a directory if it does not already exist."""
    os.makedirs(path, exist_ok=True)


def get_survey_prefixes_for_year(survey: str, year: int) -> list[str]:
    """Return all possible filename prefixes for a survey in the given year."""
    year_full = f"{year:04d}"
    year_short = f"{year % 100:02d}"
    configured_prefixes = HISTORICAL_SURVEY_MAP.get(survey, [survey])

    prefixes: list[str] = []
    for prefix in configured_prefixes:
        prefix_upper = prefix.upper()
        prefixes.append(f"{prefix_upper}{year_full}")
        prefixes.append(f"{prefix_upper}{year_short}")

    return prefixes


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


def parse_year_links(soup: BeautifulSoup, year: int) -> dict:
    """Parse the year's HTML and choose the appropriate data and dictionary links."""
    results: dict[str, dict[str, dict]] = {}
    links = soup.find_all('a', href=True)
    if not links:
        print(f"WARNING: No download links found for {year}.")
        return results

    prefix_map: dict[str, str] = {}
    for survey_code in SURVEYS_TO_DOWNLOAD:
        for prefix in get_survey_prefixes_for_year(survey_code, year):
            prefix_map[prefix] = survey_code

    prefixes = list(prefix_map.keys())
    # Sort by length so that longer, more specific prefixes (e.g., SFA2004)
    # are evaluated before shorter ones that could otherwise capture the same
    # file (e.g., S2004).
    prefixes.sort(key=len, reverse=True)

    for link in links:
        href = link['href']
        full_url = urljoin(BASE_URL, href)
        if '/ipeds/datacenter/data/' not in full_url.lower():
            continue
        parsed = urlparse(full_url)
        filename = os.path.basename(parsed.path)
        if not filename:
            continue

        filename_upper = filename.upper()

        survey_match = None
        for prefix in prefixes:
            if filename_upper.startswith(prefix):
                survey_match = prefix_map[prefix]
                break

        if survey_match is None:
            continue

        survey = survey_match
        entry_type = 'dict' if '_DICT' in filename_upper else 'data'
        is_revision = '_RV' in filename_upper
        revision_priority = 1 if is_revision else 0
        ext = os.path.splitext(filename)[1].lower()
        if entry_type == 'dict':
            ext_priority = DICT_EXTENSION_PRIORITY.get(ext, 0)
        else:
            ext_priority = 0

        results.setdefault(survey, {'data': None, 'dict': None})
        existing = results[survey][entry_type]
        candidate = {
            'priority': (revision_priority, ext_priority),
            'url': full_url,
            'filename': filename,
            'is_revision': is_revision,
        }

        if existing is None or candidate['priority'] > existing['priority']:
            results[survey][entry_type] = candidate

    return results


def download_file(session: requests.Session, url: str, destination: str) -> bool:
    """Download a file from the provided URL to the destination path."""
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
    except requests.RequestException as exc:
        print(f"ERROR: Failed to download {url}: {exc}")
    except OSError as exc:
        print(f"ERROR: Unable to write file {destination}: {exc}")
    return False


def unzip_and_remove(zip_path: str, extract_to: str) -> None:
    """Extract a zip file to the target directory and delete the original archive."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as archive:
            archive.extractall(extract_to)
        os.remove(zip_path)
        print(f"Unzipped and removed {os.path.basename(zip_path)}")
    except zipfile.BadZipFile:
        print(f"WARNING: {os.path.basename(zip_path)} is not a valid zip file.")
    except OSError as exc:
        print(f"ERROR: Unable to process {zip_path}: {exc}")


def process_year(session: requests.Session, year: int) -> None:
    """Process downloads for a single year, handling all configured surveys."""
    print(f"\n>>> Processing Year {year}...")
    soup = fetch_year_page(session, year)
    if soup is None:
        return

    year_links = parse_year_links(soup, year)
    year_dir = os.path.join(DOWNLOAD_DIR, str(year))
    ensure_directory(year_dir)

    for survey in SURVEYS_TO_DOWNLOAD:
        survey_links = year_links.get(survey, {})

        data_entry = survey_links.get('data') if survey_links else None
        dict_entry = survey_links.get('dict') if survey_links else None

        if data_entry is None:
            print(f"WARNING: Data file for survey {survey} not found for {year}.")
        else:
            filename = data_entry['filename']
            if data_entry['is_revision']:
                print(f"Downloading {filename}... (Prioritizing revised file)")
            else:
                print(f"Downloading {filename}...")
            destination = os.path.join(year_dir, filename)
            if download_file(session, data_entry['url'], destination):
                if destination.lower().endswith('.zip'):
                    print(f"Unzipping {filename}...")
                    unzip_and_remove(destination, year_dir)
                time.sleep(1)

        if dict_entry is None:
            print(f"WARNING: Dictionary for survey {survey} not found for {year}.")
        else:
            filename = dict_entry['filename']
            if dict_entry['is_revision']:
                print(f"Downloading {filename}... (Prioritizing revised file)")
            else:
                print(f"Downloading {filename}...")
            destination = os.path.join(year_dir, filename)
            if download_file(session, dict_entry['url'], destination):
                if destination.lower().endswith('.zip'):
                    print(f"Unzipping {filename}...")
                    unzip_and_remove(destination, year_dir)
                time.sleep(1)


def main() -> None:
    ensure_directory(DOWNLOAD_DIR)
    with requests.Session() as session:
        session.headers.update(HEADERS)
        for year in YEARS_TO_DOWNLOAD:
            process_year(session, year)


if __name__ == '__main__':
    main()
