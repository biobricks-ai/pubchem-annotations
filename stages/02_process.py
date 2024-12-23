import os
import json
import numpy as np
import pandas as pd

# Load the heading data
heading_df = pd.read_csv('cache/01_download/headings.csv')  # source, heading, type
root_folder = 'cache/01_download'
compound_df = heading_df[heading_df['type'] == 'Compound']

data_list = []

# Helper function to find all JSON files recursively
def find_json_files(directory):
    json_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
                print(f"Found JSON file: {os.path.join(root, file)}")
    return json_files

# Find all JSON files in the root_folder
all_json_files = find_json_files(root_folder)

# Create a dictionary for quick lookup
json_file_dict = {os.path.splitext(os.path.basename(file))[0]: file for file in all_json_files}

# Debugging: Log the total number of JSON files found
print(f"Total JSON files found: {len(all_json_files)}")

# Iterate over the compound DataFrame
for index, row in compound_df.iterrows():
    source = row['source']
    heading = row['heading']
    data_type = row['type']
    # Construct the expected file name without path
    file_name = f"{heading}.json"
    # Check if the file is in the dictionary
    if heading in json_file_dict:
        file_path = json_file_dict[heading]
        # Debugging: Log the found file path
        print(f"Processing file: {file_path}")
        with open(file_path, 'r', errors='ignore') as f:
            try:
                json_data = json.load(f)
                if isinstance(json_data, list):
                    for item in json_data:
                        if 'Data' in item:
                            for data in item['Data']:
                                # Extract PubChem identifiers
                                pubchem_cid = item.get('LinkedRecords', {}).get('CID', [])
                                pubchem_sid = item.get('LinkedRecords', {}).get('SID', [])
                                # Create a record with the necessary details
                                record = {
                                    'ANID': item.get('ANID'),
                                    'SourceName': item.get('SourceName', ''),
                                    'SourceID': item.get('SourceID', ''),
                                    'Name': item.get('Name', ''),
                                    'Description': item.get('Description', ''),
                                    'URL': item.get('URL', ''),
                                    'LicenseNote': item.get('LicenseNote', ''),
                                    'LicenseURL': item.get('LicenseURL', ''),
                                    'heading': heading,
                                    'type': data_type,
                                    'DataName': data.get('Name', ''),
                                    'Data': json.dumps(data, default=lambda x: x.tolist() if isinstance(x, np.ndarray) else x),
                                    'PubChemCID': pubchem_cid,
                                    'PubChemSID': pubchem_sid
                                }
                                
                                data_list.append(record)
                else:
                    print(f"Unexpected structure in file {file_path}")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from file {file_path}: {e}")
            except UnicodeDecodeError as e:
                print(f"Unicode decode error for file {file_path}: {e}")
    else:
        # Debugging: Log the missing file
        print(f"File {file_name} not found in any subdirectory.")


# Convert the data_list to a DataFrame
df = pd.DataFrame(data_list)
# some of the data is in ndarrays, we need to convert that to arrays
df = df.apply(lambda x: x.array if isinstance(x, np.ndarray) else x)

df.to_parquet('brick/annotations.parquet')


# Group by the first column and count
grouped_df = df.groupby(df.columns[0]).size().reset_index(name='count').sort_values('count', ascending=False)
