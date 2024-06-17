import os
import json
import pandas as pd
import re
from tqdm import tqdm

# Load the headings DataFrame
heading_df = pd.read_csv('cache/01_download/headings.csv')  # source, heading, type
root_folder = 'cache/01_download'
compound_df = heading_df[heading_df['type'] == 'Compound']

data_list = []
missing_annotation_files = []

# Helper function to extract values with markup
def extract_string_with_markup(value):
    strings = []
    if isinstance(value, dict) and 'StringWithMarkup' in value:
        for item in value['StringWithMarkup']:
            string = item.get('String', '')
            markup = item.get('Markup', [])
            strings.append({'String': string, 'Markup': markup})
    return strings

# Iterate through each row in the DataFrame with a progress bar
for index, row in tqdm(compound_df.iterrows(), total=compound_df.shape[0], desc="Processing compounds"):
    source = row['source']
    heading = row['heading']
    # Construct the expected file path
    safe_heading = re.sub(r'[\\/*?:"<>|]', '_', heading)
    subdir = os.path.join(root_folder, source)
    file_path = os.path.join(subdir, f"{safe_heading}.json")
    
    if os.path.exists(file_path):
        with open(file_path, 'r', errors='ignore') as f:
            try:
                json_data = json.load(f)
                
                # Iterate through each item in the JSON array
                for item in json_data:
                    source_name = item.get('SourceName', '')
                    source_id = item.get('SourceID', '')
                    name = item.get('Name', '')
                    description = item.get('Description', '')
                    url = item.get('URL', '')
                    license_note = item.get('LicenseNote', '')
                    license_url = item.get('LicenseURL', '')
                    
                    for data in item.get('Data', []):
                        toc_heading_type = data.get('TOCHeading', {}).get('type', '')
                        toc_heading = data.get('TOCHeading', {}).get('#TOCHeading', '')
                        data_name = data.get('Name', '')
                        value_strings = extract_string_with_markup(data.get('Value', {}))
                        
                        for value in value_strings:
                            data_list.append({
                                'SourceName': source_name,
                                'SourceID': source_id,
                                'Name': name,
                                'Description': description,
                                'URL': url,
                                'LicenseNote': license_note,
                                'LicenseURL': license_url,
                                'TOCHeadingType': toc_heading_type,
                                'TOCHeading': toc_heading,
                                'DataName': data_name,
                                'ValueString': value.get('String', ''),
                                'ValueMarkup': value.get('Markup', [])
                            })
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from file {file_path}: {e}")
            except UnicodeDecodeError as e:
                print(f"Unicode decode error for file {file_path}: {e}")
    else:
        missing_annotation_files.append(file_path)

# Save the missing annotation files for review
with open('missing_annotation_files.txt', 'w') as f:
    for item in missing_annotation_files:
        f.write("%s\n" % item)

# Convert the data list to a DataFrame and save as a parquet file
df = pd.DataFrame(data_list)
# filter the df_ECHA that only from datasource ECHA
df_echa= df[df['SourceName'] == 'European Chemicals Agency (ECHA)']
os.makedirs('brick', exist_ok=True)
output_file = 'brick/compound.parquet'
df.to_parquet(output_file)
print(f"Data compiled and saved to {output_file}")
print(f"Missing annotation files saved to missing_annotation_files.txt")
