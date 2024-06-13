import os
import json
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
# save it to a file
with open('json_file_dict.json', 'w') as f:
    json.dump(json_file_dict, f)

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
                if 'Annotations' in json_data and 'Annotation' in json_data['Annotations']:
                    # Append each annotation with heading and type to the data_list
                    for annotation in json_data['Annotations']['Annotation']:
                        annotation['heading'] = heading
                        annotation['type'] = data_type
                        data_list.append(annotation)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from file {file_path}: {e}")
            except UnicodeDecodeError as e:
                print(f"Unicode decode error for file {file_path}: {e}")
    else:
        # Debugging: Log the missing file
        print(f"File {file_name} not found in any subdirectory.")

# Convert the data_list to a DataFrame
df = pd.DataFrame(data_list)

# Ensure the output directory exists
os.makedirs('brick', exist_ok=True)
output_file = 'brick/compound.parquet'

# Save the DataFrame to a Parquet file
df.to_parquet(output_file)
print(f"Data compiled and saved to {output_file}")

# Group by the first column and count
grouped_df = df.groupby(df.columns[0]).size().reset_index(name='count').sort_values('count', ascending=False)

# Show one sample of source name "ECHA"
echa_samples = df[df['SourceName'] == 'European Chemicals Agency (ECHA)']

if not echa_samples.empty:
    echa_sample = echa_samples['Data'].iloc[0]
    print(f"Sample 1 from ECHA: {echa_sample}")
    if len(echa_samples) > 2000:
        echa_sample_2000 = echa_samples.iloc[2000]
        print(f"Sample 2000 from ECHA: {echa_sample_2000}")
    else:
        print("Less than 2000 samples available for ECHA.")
else:
    print("No data found for European Chemicals Agency (ECHA)")
