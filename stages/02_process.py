import os
import json
import pandas as pd

heading_df = pd.read_csv('cache/01_download/headings.csv') # source, heading, type
root_folder = 'cache/01_download'
compound_df = heading_df[heading_df['type'] == 'Compound']

data_list = []

for index, row in compound_df.iterrows():
    source = row['source']
    heading = row['heading']
    # Construct the expected file path
    subdir = os.path.join(root_folder, source)
    file_path = os.path.join(subdir, f"{heading}.json")
    
    if os.path.exists(file_path):
        with open(file_path, 'r', errors='ignore') as f:
            try:
                json_data = json.load(f)
                if 'Annotations' in json_data and 'Annotation' in json_data['Annotations']:
                    # Append each annotation directly to the data_list
                    for annotation in json_data['Annotations']['Annotation']:
                        data_list.append(annotation)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from file {file_path}: {e}")
            except UnicodeDecodeError as e:
                print(f"Unicode decode error for file {file_path}: {e}")


df = pd.DataFrame(data_list)
os.makedirs('brick', exist_ok=True)
output_file = 'brick/compound.parquet'
df.to_parquet(output_file)
print(f"Data compiled and saved to {output_file}")