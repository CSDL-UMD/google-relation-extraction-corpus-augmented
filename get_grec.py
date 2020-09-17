import requests
import pathlib
import os

GREC_URLS = ["https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/relation-extraction-corpus/20131104-place_of_death.json",
                 "https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/relation-extraction-corpus/20131104-date_of_birth.json",
                 "https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/relation-extraction-corpus/20131104-education-degree.json",
                 "https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/relation-extraction-corpus/20130403-institution.json",
                 "https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/relation-extraction-corpus/20130403-place_of_birth.json"]

DATA_DIR = './grec/'

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

def download_file(url, destination):
    session = requests.Session()
    response = session.get(url)
    destination += pathlib.Path(url).name
    save_response_content(response, destination)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)    

for url in GREC_URLS:
    print(f"Downloading {pathlib.Path(url).name} ...")
    download_file(url, DATA_DIR)
