# Neuropsych Summary Sheet Scrape

## Purpose

The purpose of this project is to scrape the data from "Neuropsych Summary Sheets", which are spreadsheets used by Michigan ADRC Clinical Core to (1) quickly record and calculate key neuropsychological battery scores immediately after manual scoring and (2) consolidate those scores in one place for easy human readability.

## Installation

Python 3.6 or higher is required.

* Clone the repo

```shell script
git clone git@git.umms.med.umich.edu:ldmay/neuropsych-summary-scrape.git
```

* Copy `config.cfg.template` to `config.cfg`: 

```shell script
cp config.cfg.template config.cfg
```

* Update `config.cfg` root path, regular expressions, and REDCap API credentials: 

```shell script
vim config.cfg
```

* Install necessary Python packages in your environment: 

```shell script
python3 -m pip install -r requirements.txt
```

## Use

Everything should be preconfigured in the `config.cfg` and `.json` files.

```shell script
python3 neuropsych_summary_scrape.py
```


## Script Procedure Description

1. Get list of `os.DirEntry` objects for each valid "Neuropsych Summary Sheet" (`.xlsx` extension)

2. Load parse map that details how to extract data from Neuropsych Summary Sheets (`parse_map.json`)

3. Build raw DataFrame from list of `os.DirEntry` objects

4. Clean raw DataFrame

    a. Drop dud records (e.g., missing `redcap_event_name` or `form_date` values)
   
    b. Normalize IDs (e.g., 1234 => UM00001234)
     
5. Transform clean DataFrame

    a. Reshape data for IVP & FVP NACC fields (e.g., `mocatots` => `fu_mocatots`)
    
6. Prevent premature record upload

    a. Get completed-forms records from REDCap as DataFrame
    
    b. Inner join completed-forms DataFrame and transformed DataFrame on `ptid` and `redcap_event_name`
    
7. Write joined data to CSV for manual upload via REDCap web interface (TODO: Import records directly into REDCap via its API)
