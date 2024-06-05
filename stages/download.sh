#!/bin/bash

# Create a folder to store the JSON files
mkdir -p data

# List of JSON URLs
urls=(
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?source=European%20Chemicals%20Agency%20(ECHA)&heading_type=Compound&heading=CAS&page=1&response_type=save&response_basename=PubChemAnnotations_European%20Chemicals%20Agency%20(ECHA)_heading%3DCAS"
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?source=European%20Chemicals%20Agency%20(ECHA)&heading_type=Compound&heading=GHS%20Classification&page=1&response_type=save&response_basename=PubChemAnnotations_European%20Chemicals%20Agency%20(ECHA)_heading%3DGHS%20Classification"
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?source=European%20Chemicals%20Agency%20(ECHA)&heading_type=Compound&heading=European%20Community%20(EC)%20Number&page=1&response_type=save&response_basename=PubChemAnnotations_European%20Chemicals%20Agency%20(ECHA)_heading%3DEuropean%20Community%20(EC)%20Number"
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?source=European%20Chemicals%20Agency%20(ECHA)&heading_type=Compound&heading=Hazard%20Classes%20and%20Categories&page=1&response_type=save&response_basename=PubChemAnnotations_European%20Chemicals%20Agency%20(ECHA)_heading%3DHazard%20Classes%20and%20Categories"
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?source=European%20Chemicals%20Agency%20(ECHA)&heading_type=Compound&heading=Highly%20Hazardous%20Substance&page=1&response_type=save&response_basename=PubChemAnnotations_European%20Chemicals%20Agency%20(ECHA)_heading%3DHighly%20Hazardous%20Substance"
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?source=European%20Chemicals%20Agency%20(ECHA)&heading_type=Compound&heading=Regulatory%20Information&page=1&response_type=save&response_basename=PubChemAnnotations_European%20Chemicals%20Agency%20(ECHA)_heading%3DRegulatory%20Information"
)

# Download each JSON file and save it with a filename based on the heading parameter
for url in "${urls[@]}"; do
    # Extract the heading parameter from the URL
    heading=$(echo "$url" | grep -oP '(?<=heading=)[^&]*')
    
    # Decode URL-encoded characters
    heading=$(echo "$heading" | sed 's/%20/ /g' | sed 's/%3D/=/g')
    
    # Replace spaces with underscores for the filename
    filename=$(echo "$heading" | tr ' ' '_')
    
    # Download the file and save it with the appropriate filename
    wget -O "data/${filename}.json" "$url"
done