import os
import pandas as pd
import requests
import json
from urllib.parse import urlparse

# Define the directory for images and the tracking file
IMAGE_DIR = "plane-images"
TRACKING_FILE = os.path.join(IMAGE_DIR, "downloaded_images.json")
CSV_FILES_PATTERN = "plane-alert-*-images.csv"
DOWNLOAD_LIMIT = None  # Limit for testing as requested

def setup_directories():
    """Ensures the image directory exists."""
    os.makedirs(IMAGE_DIR, exist_ok=True)

def load_tracking_data():
    """Loads previously downloaded image data from the tracking file."""
    if os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_tracking_data(data):
    """Saves current image data to the tracking file."""
    with open(TRACKING_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def append_tracking_data(url, image_info):
    """Appends a single image entry to the tracking file."""
    downloaded_images = load_tracking_data()
    downloaded_images[url] = image_info
    save_tracking_data(downloaded_images)

def download_image(url, save_path):
    """Downloads an image from a URL and saves it to a specified path."""
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded: {url} to {save_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False

def get_image_filename(url):
    """Generates a filename from a URL, preserving the original extension."""
    path = urlparse(url).path
    name = os.path.basename(path)
    if '.' in name:
        return name
    return name + ".jpg" # Default to .jpg if no extension

def main():
    setup_directories()
    downloaded_images = load_tracking_data()
    
    # Find all relevant CSV files in parent directory
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_files = [f for f in os.listdir(parent_dir) if f.startswith("plane-alert-") and f.endswith("-images.csv")]
    
    newly_downloaded_count = 0
    processed_count = 0

    for csv_file in csv_files:
        print(f"Processing {csv_file}...")
        try:
            csv_path = os.path.join(parent_dir, csv_file)
            df = pd.read_csv(csv_path)
            
            for col in ['#ImageLink', '#ImageLink2', '#ImageLink3', '#ImageLink4']:
                if col in df.columns:
                    for _, row in df.iterrows():
                        url = row.get(col)
                        icao = row.get('$ICAO', '')
                        
                        if pd.notna(url) and url and url not in downloaded_images:
                            filename = get_image_filename(url)
                            save_path = os.path.join(IMAGE_DIR, filename)
                            
                            if download_image(url, save_path):
                                image_info = {
                                    'local_path': save_path,
                                    'icao': icao,
                                    'column': col,
                                    'csv_file': csv_file,
                                    'filename': filename
                                }
                                append_tracking_data(url, image_info)
                                downloaded_images[url] = image_info  # Keep local cache
                                newly_downloaded_count += 1
                                print(f"Downloaded and tracked: {filename} (ICAO: {icao})")
                            else:
                                print(f"Failed to download {url}")
                        
                        processed_count += 1
                        if DOWNLOAD_LIMIT and newly_downloaded_count >= DOWNLOAD_LIMIT:
                            print(f"Reached download limit of {DOWNLOAD_LIMIT} images.")
                            break
                    
                    if DOWNLOAD_LIMIT and newly_downloaded_count >= DOWNLOAD_LIMIT:
                        break
                        
            if DOWNLOAD_LIMIT and newly_downloaded_count >= DOWNLOAD_LIMIT:
                break
                
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")

    print(f"Download process completed. Newly downloaded images: {newly_downloaded_count}")

if __name__ == "__main__":
    main()
