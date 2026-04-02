import urllib.request
import os

base_url = "https://alexandria.icams.rub.de/data/pbe/2024.12.15/alexandria_{:03d}.json.bz2"
output_dir = "alexandria_data"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for i in range(1, 51):
    url = base_url.format(i)
    filename = os.path.join(output_dir, url.split('/')[-1])
    if os.path.exists(filename):
        print(f"{filename} already exists. Skipping...")
        continue
    
    print(f"Downloading {url} to {filename}...")
    try:
        urllib.request.urlretrieve(url, filename)
        print(f"Successfully downloaded {filename}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")
