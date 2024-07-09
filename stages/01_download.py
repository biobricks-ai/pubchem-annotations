# Imports
# -------
import re
import os
import json
import tqdm
import time
import requests
import pandas as pd
from urllib.parse import urlencode

# GET DATA SOURCES
# This step retrieves all the pubchem data sources that have annotations

os.makedirs('cache/01_download', exist_ok=True)
csv_filename = 'cache/01_download/PubChemDataSources_all.csv'
with open(csv_filename, 'wb') as file:
  res = requests.get("https://pubchem.ncbi.nlm.nih.gov/rest/pug/sourcetable/all/CSV/?response_type=save&response_basename=PubChemDataSources_all")
  file.write(res.content)

# Filter to sources with `Annotation Count` > 0
source_names = pd.read_csv(csv_filename, encoding='latin1').query("`Annotation Count` > 0")['Source Name'].unique()

# DOWNLOAD SOURCE ANNOTATIONS
# Each source has a download all button that we can use to download all the annotations for that source

base_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/annotations/sourcename/JSON"
links = []
for name in source_names:
  args = urlencode({'sourcename': name, 'response_type': 'save', 'response_basename': f'{name}_PubChemAnnotationTopics'})
  links += [f"{base_url}?{args}"]

# Download links to cache/{source_name}/annotations.json
safe_sources = [re.sub(r'[\\/*?:"<>|]', '_', name) for name in source_names]
# for link, name in tqdm.tqdm(zip(links, safe_sources), total=len(links), desc="Downloading annotations"):
#   annotation_path = os.path.join('cache/01_download', name, 'annotations.json')
#  os.makedirs(os.path.dirname(annotation_path), exist_ok=True)
#  response = requests.get(link, stream=True)
#  time.sleep(3)  # Be nice to the server
#  with open(annotation_path, 'wb') as f:
#    for chunk in response.iter_content(chunk_size=8192):
#      _ = f.write(chunk)

# Make a 'headings' table by reading all the annotations.json files
headings = []
for name, safe_name in zip(source_names, safe_sources):
  annotation_path = os.path.join('cache/01_download', safe_name, 'annotations.json')
  with open(annotation_path, 'r') as file:
    data = json.load(file)
    if 'InformationList' in data and 'Annotation' in data['InformationList']:
      d = data['InformationList']['Annotation']
      headings.extend([{'source': name, 'safe_source': safe_name, 'heading': i['Heading'], 'type': i['Type']} for i in d])
    else:
      print(f"InformationList or Annotation key not found in {annotation_path}")

heading_df = pd.DataFrame(headings)  # DataFrame with 'source' 'heading' 'type'
heading_df.to_csv('cache/01_download/headings.csv', index=False)

# read heading_df
heading_df = pd.read_csv('cache/01_download/headings.csv')

# Find DTP_NCI headings in heading_df
dtp_nci_headings = heading_df[heading_df['source'] == 'DTP_NCI']

# DOWNLOAD HEADING ANNOTATIONS WITH PAGINATION
base_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?"

for index, row in tqdm.tqdm(heading_df.iterrows(), total=len(heading_df), desc="Downloading headings"):
  time.sleep(3)
  safe_heading = re.sub(r'[\\/*?:"<>|]', '_', row['heading'])
  args = {
    'source': row['source'],
    'heading_type': row['type'],
    'heading': row['heading'],
    'response_type': 'save',
    'response_basename': f'PubChemAnnotations_{row["source"]}_heading={safe_heading}'
  }

  # Initial URL and download path setup
  download_path = os.path.join('cache/01_download', row['safe_source'], f'{safe_heading}.json')
  os.makedirs(os.path.dirname(download_path), exist_ok=True)

  all_data = []
  current_page = 1

  while True:
    # Add the current page to the args and construct the URL
    args['page'] = current_page
    paginated_url = f"{base_url}{urlencode(args)}"

    try:
      # Fetch data for the current page
      response = requests.get(paginated_url)
      if response.status_code == 200:
        data = response.json()
        annotations = data.get('Annotations', {})
        all_data.extend(annotations.get('Annotation', []))
        # Check if more pages exist
        if current_page >= annotations.get('TotalPages', 1):
          break
        # Move to the next page
        current_page += 1
        time.sleep(1)  # be nice to the server
      else:
        print(f"Failed to download page {current_page} for {row['heading']} from {row['source']}")
    except Exception as e:
      print ("Stacktrace: %s" % e)
      continue

  # Save the combined data to the JSON file
  with open(download_path, 'w') as f:
    json.dump(all_data, f)
