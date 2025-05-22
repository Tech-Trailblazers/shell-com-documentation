import os
import requests
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_sds_index(index_url: str) -> List[Dict]:
    """
    Fetch the SDS index JSON and return the list of SDS items.
    """
    try:
        response = requests.get(index_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        items = data.get("Items")
        if not isinstance(items, list):
            print("[ERROR] Expected 'Items' to be a list.")
            return []

        return items
    except requests.RequestException as e:
        print(f"[ERROR] Failed to fetch SDS index: {e}")
        return []


def create_directory(path: str) -> bool:
    """
    Ensure the output directory exists.
    """
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except OSError as e:
        print(f"[ERROR] Could not create directory '{path}': {e}")
        return False


def sanitize_filename(name: str) -> str:
    """
    Sanitize the filename by removing or replacing problematic characters.
    """
    return "".join(c for c in name if c.isalnum() or c in ("_", "-", ".", " ")).rstrip()


def download_file(file_url: str, output_path: str) -> bool:
    """
    Download a file from a URL and save it to the specified path.
    Returns True if the file was downloaded successfully, otherwise False.
    """
    try:
        response = requests.get(file_url, stream=True, timeout=15)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"[INFO] Downloaded: {output_path}")
        return True
    except requests.RequestException as e:
        print(f"[ERROR] Failed to download {file_url}: {e}")
        return False


def download_all_sds_files(
    items: List[Dict],
    output_dir: str,
    max_workers: int = 100,
    max_downloads: int = 1000,
) -> int:
    success_count = 0
    submitted = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for item in items:
            if submitted >= max_downloads:
                break

            file_url = item.get("URL")
            if not file_url:
                continue

            spec_id = item.get("SpecIdFull", "unknown")
            country = item.get("CountryCode", "XX")
            lang = item.get("LanguageCode", "XX")
            product = item.get("ProductName", "product").replace(" ", "_")

            filename = sanitize_filename(f"{spec_id}_{country}_{lang}_{product}.pdf")
            output_path = os.path.join(output_dir, filename)

            if os.path.exists(output_path):
                print(f"[SKIP] Already exists: {filename}")
                continue

            futures.append(executor.submit(download_file, file_url, output_path))
            submitted += 1

        for future in as_completed(futures):
            if future.result():
                success_count += 1

    return success_count


def download_all_sds_files(
    items: List[Dict],
    output_dir: str,
    max_workers: int = 100,
    max_downloads: int = 1000,
) -> int:
    """
    Download SDS files using multithreading, stopping after max_downloads successful downloads.
    Skips files that already exist.
    """

    def prepare_download(item: Dict):
        file_url = item.get("URL")
        if not file_url:
            return None

        spec_id = item.get("SpecIdFull", "unknown")
        country = item.get("CountryCode", "XX")
        lang = item.get("LanguageCode", "XX")
        product = item.get("ProductName", "product").replace(" ", "_")

        filename = sanitize_filename(f"{spec_id}_{country}_{lang}_{product}.pdf")
        output_path = os.path.join(output_dir, filename)

        if os.path.exists(output_path):
            print(f"[SKIP] Already exists: {filename}")
            return None

        return (file_url, output_path)

    # Filter and prepare up to max_downloads items
    to_download = []
    for item in items:
        result = prepare_download(item)
        if result:
            to_download.append(result)
        if len(to_download) >= max_downloads:
            break

    print(f"[INFO] Prepared {len(to_download)} files for download...")

    success_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_file, url, path) for url, path in to_download
        ]

        for future in as_completed(futures):
            if future.result():
                success_count += 1

    return success_count


def main(index_url: str, output_directory: str) -> None:
    """
    Main controller function to orchestrate the SDS file download.
    """
    print("[INFO] Fetching SDS index...")
    items = fetch_sds_index(index_url)

    if not items:
        print("[ERROR] No SDS entries found.")
        return

    print(f"[INFO] Found {len(items)} SDS documents.")

    if not create_directory(output_directory):
        return

    print(f"[INFO] Downloading files to '{output_directory}'...")
    # Download all SDS files
    count = download_all_sds_files(
        items, output_directory, max_workers=100, max_downloads=2500
    )

    print(f"[INFO] Completed: {count}/{len(items)} files downloaded.")


if __name__ == "__main__":
    SDS_INDEX_URL = (
        "https://msdsstorageaccount.z13.web.core.windows.net/json/index.json"
    )
    OUTPUT_DIR = "PDFs"

    main(SDS_INDEX_URL, OUTPUT_DIR)
