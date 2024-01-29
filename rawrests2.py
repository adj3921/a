from pathlib import Path

import requests
import json
from pprint import pprint

endpoint_base = "https://api.semanticscholar.org/"
datasets_base = endpoint_base + "datasets/v1/"
use_release = "2023-03-07"
#use_release = "2024-01-24"

cur_dir = Path(__file__).parent.absolute()

def get_api_key():
    with open("apikey.txt") as f:
        return f.read().strip()


def get_all_releases():
    # query datasets/v1/release/ using the API key
    endpoint = datasets_base + "release/"
    headers = {"x-api-key": get_api_key()}
    response = requests.get(endpoint, headers=headers)
    return json.loads(response.text)


def list_release_datasets(release):
    # query datasets/v1/release/{release}/datasets/ using the API key
    endpoint = datasets_base + f"release/{release}"
    headers = {"x-api-key": get_api_key()}
    response = requests.get(endpoint, headers=headers)
    return json.loads(response.text)


def get_download_links(release, dataset):
    # query https://api.semanticscholar.org/datasets/v1/release/{release_id}/dataset/{dataset_name}
    endpoint = datasets_base + f"release/{release}/dataset/{dataset}"
    headers = {"x-api-key": get_api_key()}
    response = requests.get(endpoint, headers=headers)
    return json.loads(response.text)


def download_dataset(destination_dir: Path, dataset_name: str, limit_num_files: int = 1):
    start_links = get_download_links(use_release, dataset_name)['files']
    links = start_links
    for i, link_url in enumerate(start_links):
        if limit_num_files and i >= limit_num_files:
            break
        link_url = links[i]
        print(f"Downloading {dataset_name} {i} to {destination_dir}")
        # download the file into destination_dir showing the progress
        import requests
        import shutil
        from tqdm import tqdm
        dest = destination_dir / f"{dataset_name}_{use_release}_{i}.jsonl.gz"
        dest.parent.mkdir(exist_ok=True, parents=True)
        # download showing amount downloaded using tqdm
        with requests.get(link_url, stream=True) as r:
            r.raise_for_status()
            with open(dest, 'wb') as f:
                total_length = int(r.headers.get('content-length'))
                for chunk in tqdm(
                    r.iter_content(chunk_size=8192), total=total_length // 8192, unit='KB',
                    desc=f"Downloading {dataset_name} {i}/{len(links)} to {destination_dir}"
                ):
                    if chunk:
                        f.write(chunk)
                        f.flush()
        # refresh the links in case they expire
        links = get_download_links(use_release, dataset_name)['files']
        assert len(links) == len(start_links)


def download_s2orc(limit_num_files: int = 1):
    download_dataset(cur_dir / "data/jan24" / "s2orc/raw", "s2orc", limit_num_files=limit_num_files)


def download_papers_db(limit_num_files: int = 1):
    download_dataset(cur_dir / "data/jan24" / "papers/raw", "papers", limit_num_files=limit_num_files)


def download_venues(limit_num_files: int = 1):
    download_dataset(cur_dir / "data/jan24" / "publication-venues/raw", "publication-venues", limit_num_files=limit_num_files)


def main():
    print(get_all_releases())
    print(use_release)
    #exit()
    pprint(list_release_datasets(use_release)['datasets'])
    #exit()
    #pprint(get_download_links(use_release, "s2orc"))
    #pprint(get_download_links(use_release, "publication-venues"))
    #pprint(get_download_links(use_release, "publication-venues"))
    #download_s2orc(cur_dir / "data" / "s2orc/raw", limit_num_files=1)
    #download_s2orc(limit_num_files=None)
    download_papers_db(limit_num_files=None)
    #download_venues(limit_num_files=None)


if __name__ == "__main__":
    main()