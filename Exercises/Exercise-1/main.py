
import os
import requests
import zipfile
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]
DOWNLOAD_DIR = "downloads"

def download_and_extract(uri):
    try:
        filename = uri.split("/")[-1]
        zip_path = os.path.join(DOWNLOAD_DIR, filename)

        # Gửi request
        print(f"Downloading: {uri}")
        response = requests.get(uri)
        response.raise_for_status()

        # Lưu zip file
        with open(zip_path, "wb") as f:
            f.write(response.content)

        # Giải nén
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)

        # Xóa file zip
        os.remove(zip_path)
        print(f"Done: {filename}")

    except Exception as e:
        print(f"Failed: {uri} - {e}")

def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    for uri in download_uris:
        download_and_extract(uri)
if __name__ == "__main__":
    main()
