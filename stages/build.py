import os
import json
import pandas as pd
from pandas import json_normalize

# Define the input and output directories
input_dir = 'data'  # Assuming the JSON files are in a folder named 'json'
output_dir = 'parquet'  # Output folder for Parquet files

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Function to process each JSON file
def process_json_file(filepath):
    with open(filepath, 'r') as file:
        data = json.load(file)
    
    # Normalize the nested structure
    df = json_normalize(data['Annotations']['Annotation'])
    
    return df

# Process each JSON file in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith('.json'):
        print(f"Processing {filename}")
        json_path = os.path.join(input_dir, filename)
        
        # Process the JSON file to get a DataFrame
        df = process_json_file(json_path)
        
        # Construct the output Parquet file path
        parquet_filename = os.path.splitext(filename)[0] + '.parquet'
        parquet_path = os.path.join(output_dir, parquet_filename)
        
        # Save the DataFrame to a Parquet file
        df.to_parquet(parquet_path, index=False)
        print(f"Processed {json_path} and saved to {parquet_path}")

print("All JSON files have been processed and saved as Parquet files.")


