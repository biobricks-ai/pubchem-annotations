# write a test that checks whether some common annotation data is present in the brick
import pandas as pd
from tqdm import tqdm
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, array_contains
from pyspark.sql.types import BooleanType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BenzeneAnnotations") \
    .config("spark.driver.memory", "64g") \
    .config("spark.executor.memory", "64g") \
    .config("spark.driver.maxResultSize", "48g") \
    .getOrCreate()

# Read the parquet file using Spark
spark_df = spark.read.parquet('brick/annotations.parquet').cache()

spark_df_benzene = spark_df.filter(array_contains(spark_df.PubChemCID, 241))
df_benzene = spark_df_benzene.toPandas()

# Display the results
print(f"Number of annotations for CID 241: {len(df_benzene)}")
print("\nAnnotations for CID 241:")
for _, row in df_benzene.iterrows():
    print(f"Source: {row['SourceName']}")
    print(f"Name: {row['Name']}")
    print(f"Description: {row['Description'][:100]}...")  # Truncate long descriptions
    print(f"Type: {row['type']}")
    print(f"Data: {json.dumps(row['Data'], indent=2)}")
    print("---")

# Function to extract all keys from nested dictionaries
def extract_keys(data, prefix=''):
    keys = set()
    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key
            keys.add(full_key)
            keys.update(extract_keys(value, full_key))
    elif isinstance(data, list):
        for item in data:
            keys.update(extract_keys(item, prefix))
    return keys

# Collect all unique keys
all_keys = set()
for _, row in df_benzene.iterrows():
    data = json.loads(row['Data'])
    all_keys.update(extract_keys(data))

# Count occurrences of each key
key_counts = {key: 0 for key in all_keys}
for _, row in df_benzene.iterrows():
    data = json.loads(row['Data'])
    for key in extract_keys(data):
        key_counts[key] += 1

# Sort keys by frequency
sorted_keys = sorted(key_counts.items(), key=lambda x: x[1], reverse=True)

print("\nCommon fields and subfields in row['Data']:")
for key, count in sorted_keys:
    print(f"{key}: {count} occurrences")

# Optionally, you can set a threshold to show only the most common fields
# For example, to show fields that appear in at least 10% of the rows:
threshold = len(df_benzene) * 0.1
print("\nFields appearing in at least 10% of the rows:")
for key, count in sorted_keys:
    if count >= threshold:
        print(f"{key}: {count} occurrences")

import json
from pprint import pprint

def pretty_print_data(data):
    if isinstance(data, str):
        data = json.loads(data)
    
    print("TOC Heading:", data.get('TOCHeading', {}).get('#TOCHeading', 'N/A'))
    print("Type:", data.get('TOCHeading', {}).get('type', 'N/A'))
    
    if 'Value' in data:
        value = data['Value']
        if isinstance(value, dict):
            if 'StringWithMarkup' in value:
                print("Value:")
                for item in value['StringWithMarkup']:
                    print("  -", item.get('String', 'N/A'))
                    if 'Markup' in item:
                        for markup in item['Markup']:
                            print(f"    * Markup: {markup.get('Type', 'N/A')} "
                                  f"(Start: {markup.get('Start', 'N/A')}, "
                                  f"Length: {markup.get('Length', 'N/A')})")
                            if 'URL' in markup:
                                print(f"      URL: {markup['URL']}")
            elif 'Number' in value:
                print(f"Value: {value['Number']}")
            elif 'ExternalDataURL' in value:
                print(f"External Data URL: {value['ExternalDataURL']}")
            elif 'Binary' in value:
                print(f"Binary Data: [binary data of length {len(value['Binary'])}]")
        else:
            print(f"Value: {value}")
    
    if 'Reference' in data:
        print("Reference:", data['Reference'])
    
    if 'Description' in data:
        print("Description:", data['Description'])
    
    if 'Name' in data:
        print("Name:", data['Name'])
    
    if 'URL' in data:
        print("URL:", data['URL'])

# Use this function in your main loop
for _, row in df_benzene.iterrows():
    print(f"\nSource: {row['SourceName']}")
    print(f"Annotation Type: {row['type']}")
    pretty_print_data(json.loads(row['Data']))
    print("---")
    
# Stop the Spark session
spark.stop()
