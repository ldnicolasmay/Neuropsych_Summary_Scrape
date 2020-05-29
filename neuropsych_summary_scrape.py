#!/usr/bin/env python3

# Import modules
import configparser
import json
import os
from re import compile
import pandas as pd
from datetime import date

from regex_target_dir_entries import *
from neuropsych_summary_scrape_helpers import *


def main():

    # Read config
    print("Parsing config file...")
    config = configparser.ConfigParser()
    config.read("resources/config/config.cfg")
    box_json_config_path = config.get('base', 'box_json_config_path')
    box_folder_id = config.get('base', 'box_folder_id')
    config_iter_sections = [section for section in config.sections() if section != 'base']
    subdirs_regex_list = [config.get(section, 'subdirs_regex') for section in config_iter_sections]
    xlsx_regex_list = [config.get(section, 'xlsx_regex') for section in config_iter_sections]

    # Join and compile config regexes
    print("Processing regexes...")
    subdirs_regex_str = "|".join(subdirs_regex_list)
    xlsx_regex_str = "|".join(xlsx_regex_list)
    subdirs_regex = compile(subdirs_regex_str)
    xlsx_regex = compile(xlsx_regex_str)

    # Preload DataFrames from REDCap for studies
    print("Retrieving REDCap data...")
    with open("resources/json/redcap_fields.json", "r") as redcap_fields_file:
        redcap_fields_data = redcap_fields_file.read()
    redcap_fields_dict = json.loads(redcap_fields_data)
    ummap_redcap_fields = redcap_fields_dict['ummap']
    electra_redcap_fields = redcap_fields_dict['electra']
    ummap_df = retrieve_redcap_dataframe(config.get('ummap', 'redcap_api_uri'),
                                         config.get('ummap', 'redcap_project_token'),
                                         ummap_redcap_fields, vp=False)
    electra_df = retrieve_redcap_dataframe(config.get('electra', 'redcap_api_uri'),
                                           config.get('electra', 'redcap_project_token'),
                                           electra_redcap_fields)

    # Load parse map json file as dict
    with open("resources/json/parse_map.json", "r") as parse_map_file:
        parse_map_json_data = parse_map_file.read()
    parse_map_dict = json.loads(parse_map_json_data)

    # Get authenticated Box client; get root Box folder
    print("Authenticating Box client...")
    box_client = get_box_authenticated_client(box_json_config_path)
    root_box_dir = box_client.folder(folder_id=box_folder_id).get()

    # Get list of summary sheet Box subitems
    print("Retrieving Neuropsych Summary Sheets from Box...")
    summ_sheet_box_items_list = \
        extract_regexed_box_subitems(root_box_dir,
                                     subdirs_regex,
                                     xlsx_regex,
                                     ("type", "id", "sequence_id", "etag", "name", "path_collection"))

    # Loop over summary sheet DirEntries and process
    print("Building raw dataframe...")
    raw_df = box_build_accum_df(summ_sheet_box_items_list, parse_map_dict, electra_df)

    # Normalize UMMAP IDs
    print("Cleaning dataframe...")
    clean_df = raw_df.copy().dropna(subset=['redcap_event_name'])
    clean_df['ptid'] = clean_df['ptid'].apply(normalize_ummap_id)

    # Reörder columns
    clean_df_cols = clean_df.columns.tolist()
    front_cols = ['ptid', 'redcap_event_name']
    back_cols = [col for col in clean_df_cols if col not in front_cols]
    clean_df_cols = front_cols + back_cols
    clean_df = clean_df[clean_df_cols]

    # Add "fu_" prefix to NACC columns for follow-up visits
    print("Transforming dataframe...")
    with open("resources/json/nacc_fields.json", "r") as nacc_fields_json_file:
        nacc_fields_json_data = nacc_fields_json_file.read()
    nacc_fields_dict = json.loads(nacc_fields_json_data)
    nacc_cols = nacc_fields_dict['nacc_cols']
    transformed_df = add_prefix_to_fu_visits(clean_df, nacc_cols, "fu_")

    # Ensure integer column types
    for col, col_spec in parse_map_dict.items():
        if col_spec['dtype'] == 'int':
            transformed_df[col] = transformed_df[col].astype('Int64')

    # Get records with forms marked as completed
    ummap_df_ivp_complete = get_ivp_complete(ummap_df)
    ummap_df_fvp_complete = get_fvp_complete(ummap_df)
    completed_forms_df = \
        ummap_df.loc[(ummap_df.header_complete == "2") & (ummap_df_ivp_complete | ummap_df_fvp_complete),
                     ['ptid', 'redcap_event_name']]

    # Avoid uploading records with incomplete forms by inner join of completed_forms_df and transformed_df
    print("Filtering dataframe for only those with complete REDCap records...")
    importable_df = pd.merge(completed_forms_df, transformed_df, how='inner', on=['ptid', 'redcap_event_name'])

    # Write dataframe to CSV
    print("Writing CSV to file...")
    importable_csv_path = "data/csv"
    importable_csv_filename = f"neuropsych_scrape_data-{date.today().isoformat()}.csv"
    importable_df.to_csv(f"{importable_csv_path}/{importable_csv_filename}", index=False)

    # Import CSV records to REDCap
    print("Importing CSV to REDCap...")
    with open(f"{importable_csv_path}/{importable_csv_filename}", "r") as importable_csv_data_file:
        importable_csv_data = importable_csv_data_file.read()
        import_redcap_data(config.get('ummap', 'redcap_api_uri'),
                           config.get('ummap', 'redcap_project_token'),
                           importable_csv_data, vp=False)

    print("Done.")


if __name__ == "__main__":
    main()
