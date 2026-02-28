# OpenINSPIRE - library for downloading and amalgamating INSPIRE land parcels

## Overview
The `OpenINSPIRE` library provides a simple command line interface for automatically downloading and amalgamating INSPIRE land parcels. This data is typically provided as separate `GML` files for each UK local authority.

A `yml` configuration file is used to provide key parameters to the library.

## Key features

- Uses `BeautifulSoup` to download and parse contents of main INSPIRE webpage which contains link to `GML` INSPIRE land parcel files for each UK local authority.
- Downloads and unzips each `zip` file.
- Converts all `GML` files into `GPKG` and amalgamates into single `GPKG` file.

## Installation

```
pip install git+https://github.com/opensiteenergy/openinspire.git
```

To use the library, enter:

```
openinspire inspire.yml
```

## Configuration file

The `.yml` configuration file should have the following format:

```
# ----------------------------------------------------
# inspire.yml
# yml configuration file for downloading INSPIRE land parcels
# ----------------------------------------------------

# Link to this GitHub code repository 
# This can be used to host yml files on an open data server and automatically install required library just-in-time
codebase:
  https://github.com/opensiteenergy/openinspire.git

# Link to Inspire webpages
sources:
  england-wales:
    url: "https://use-land-property-data.service.gov.uk/datasets/inspire/download"
    strategy: "simple_web"
    ext: ".zip"
    match_text: ["gml", "land"]

# Directory where temporary data is stored
cache_dir:
 cache/inspire-land-parcels--england-wales

# The exact name and extension of the final file generated
output:
  solar-land-parcels-too-small--england-wales.gpkg

# Filters the final result values by area in hectares
area:
  # Maximum area
  max: 
    1
```

