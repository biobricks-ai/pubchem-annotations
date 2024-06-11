import requests, os, pandas as pd, json, tqdm, re, time
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

# GET DATA SOURCES ======================================================================================================
# this step retrieves all the pubchem data sources that have annotations (see https://pubchem.ncbi.nlm.nih.gov/source/#sort=Last-Updated-Latest-First&type=Annotations) 

os.makedirs('cache/01_download', exist_ok=True)
csv_filename = 'cache/01_download/PubChemDataSources_all.csv'
with open(csv_filename, 'wb') as file:
    res = requests.get("https://pubchem.ncbi.nlm.nih.gov/rest/pug/sourcetable/all/CSV/?response_type=save&response_basename=PubChemDataSources_all")
    file.write(res.content)

# filter to sources with `Annotation Count` > 0
source_names = pd.read_csv(csv_filename).query("`Annotation Count` > 0")['Source Name'].unique()

# DOWNLOAD SOURCE ANNOTATIONS ============================================================================================
# each source has a download all button that we can use to download all the annotations for that source

base_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/annotations/sourcename/JSON"
links = []
for name in source_names:
    args = urlencode({'sourcename':name, 'response_type':'save', 'response_basename':f'{name}_PubChemAnnotationTopics'})
    links += [f"{base_url}?{args}"]

# download links to cache/{source_name}/annotations.json
safe_sources = [re.sub(r'[\\/*?:"<>|]', '_', name) for name in source_names]
for link, name in tqdm.tqdm(zip(links, safe_sources), total=len(links), desc="Downloading annotations"):
    annotation_path = os.path.join('cache/01_download', name, 'annotations.json')
    os.makedirs(os.path.dirname(annotation_path), exist_ok=True)
    response = requests.get(link, stream=True)
    time.sleep(1)  # be nice to the server
    with open(annotation_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            _ = f.write(chunk)

# make a 'headings' table by reading all the annotations.json files
headings = []
for name in safe_sources:
    annotation_path = os.path.join('cache/01_download', name, 'annotations.json')
    with open(annotation_path, 'r') as file:
        d = json.load(file)['InformationList']['Annotation']
        headings.extend([{'source': name, 'heading': i['Heading'], 'type': i['Type']} for i in d])
heading_df = pd.DataFrame(headings) # df with 'source' 'heading' 'type'
heading_df.to_csv('cache/01_download/headings.csv', index=False)

# download the headings 
base_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?"
for index, row in tqdm.tqdm(heading_df.iterrows(), total=len(heading_df), desc="Downloading headings"):
    time.sleep(1)  # be nice to the server
    safe_heading = re.sub(r'[\\/*?:"<>|]', '_', row['heading'])
    args = urlencode({'source': row['source'],  'heading_type': row['type'], 'heading': row['heading'], 'response_type': 'save', 'response_basename': f'PubChemAnnotations_{row["source"]}_heading={safe_heading}'})
    
    # Construct the full URL and download path
    heading_url = f"{base_url}{args}"
    download_path = os.path.join('cache/01_download', row['source'], f'{safe_heading}.json')
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    # Download and save the heading data
    response = requests.get(heading_url)
    if response.status_code == 200:
        with open(download_path, 'wb') as f:
            _ = f.write(response.content)