import pandas as pd

# read the headings.csv from cache/01_download/headings.csv
heading_df = pd.read_csv('cache/01_download/headings.csv') # source, heading, type

# TODO figure out a general way to parse all the 'Compound' .json files and store them in a parquet file
# Parse annotations.json to get headings
