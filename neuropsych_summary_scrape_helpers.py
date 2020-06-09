import configparser
import logging
import requests
import pandas as pd
import os
import sys
from re import match, search
from datetime import datetime
from boxsdk import JWTAuth, Client


def get_logger(app_path):
    """
    Get a Logger object for passing to helper functions

    :param app_path: Path to app resources
    :type app_path: str
    :return: logger object
    :rtype: logging.Logger
    """
    # create logger for this module; use name other than "logger" because Box SDK uses that name
    nss_logger = logging.getLogger("neuropsych_summary_scrape")
    nss_logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(f"{app_path}/data/log/{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log")
    fh.setLevel(logging.INFO)
    # create console handler with a higher log level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    # create formatters and add them to the handlers
    fh_formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")
    ch_formatter = logging.Formatter("    %(levelname)s : %(message)s")
    fh.setFormatter(fh_formatter)
    ch.setFormatter(ch_formatter)
    # add the handlers to the logger
    nss_logger.addHandler(fh)
    nss_logger.addHandler(ch)

    return nss_logger


def return_col_row_of_val(df_to_search, search_str):
    """
    Return row and column indices (as 2-tuple of integers) of `search_str` within dataframe `df_to_search`

    :param df_to_search: DataFrame that may contain `search_str`
    :type df_to_search: pandas.DataFrame
    :param search_str: String that may exist in `df_to_search`
    :type search_str: str
    :return: tuple of indexes of `search_str` location in `df_to_search`
    :rtype: (int, int)
    """
    # loop over each df column
    for col_idx, series in df_to_search.items():
        # loop over each column's row
        for row_idx, value in series.items():
            # search for the string, return the indices early if found
            if pd.notna(value) and match(search_str, value):
                return row_idx, col_idx
    # else return None-None tuple
    return None, None


def convert_x_to_dtype(raw_value, type_str, anchor, path, nss_logger):
    """
    Converts a value to a target data type

    :param raw_value: Value to be converted
    :type raw_value: Any
    :param type_str: String of the target data type
    :type type_str: str
    :param anchor: Anchor value from `parse_map.json`
    :type anchor: str
    :param path: Path of file that `raw_value` came from
    :type path: str
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return: value as target data type
    """
    # Determine coercion function
    if type_str == "int":
        func = int
    elif type_str == "float":
        func = float
    elif type_str == "str":
        func = str
    else:
        raise ValueError(f"Unexpected type string \"{type_str}\" for `{anchor}` in parse_map.json")

    # Apply coercion function to raw_value
    try:
        value = func(raw_value)
    except ValueError as e:
        value = None
        nss_logger.warning(f"Raw value in sheet not compatible with defined dtype at {anchor} in {path}; {e}")

    return value


def local_extract_dir_visit_num(dir_entry, nss_logger):
    """
    Extract directory visit number from local spreadsheet

    :param dir_entry: Spreadsheet DirEntry object
    :type dir_entry: os.DirEntry
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return: Visit number
    :rtype: int
    """
    visit_match = search(r'.*Visit (\d+).*', dir_entry.path)
    visit_str = visit_match.group(1)
    try:
        visit_int = int(visit_str)
    except ValueError as e:
        visit_int = None
        nss_logger.warning(e)

    return visit_int


def box_extract_dir_visit_num(box_item, nss_logger):
    """

    :param box_item:
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return:
    """
    box_item_path = ""
    for path_item in box_item.path_collection['entries']:
        box_item_path += f"/{path_item.name}"

    visit_match = search(r'.*Visit (\d+).*', box_item_path)
    visit_str = visit_match.group(1)
    try:
        visit_int = int(visit_str)
    except ValueError as e:
        visit_int = None
        nss_logger.warning(e)

    return visit_int


def local_extract_dir_ummap_id(dir_entry, electra_dir_entry, nss_logger):
    """

    :param dir_entry: Spreadsheet DirEntry object
    :type dir_entry: os.DirEntry
    :param electra_dir_entry:
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return:
    """
    if not electra_dir_entry:
        id_match = search(r'.*/(\d+).[Ss]cor', dir_entry.path)
        id_str = id_match.group(1)
        try:
            id_int = int(id_str)
        except ValueError as e:
            id_int = None
            nss_logger.warning(e)
    else:
        id_match = search(r'.*/KG\d{6} - (\d{4})/KG\d{6}_(\d{4}).*', dir_entry.path)
        id_str_1 = id_match.group(1)
        id_str_2 = id_match.group(2)
        if id_str_1 == id_str_2:
            id_str = id_str_1
            try:
                id_int = int(id_str)
            except ValueError as e:
                id_int = None
                nss_logger.warning(e)
        else:
            raise AssertionError(f"UMMAP IDs from {dir_entry.path} don't match")
    ummap_id = normalize_ummap_id(id_int)

    return ummap_id


def box_extract_dir_ummap_id(box_item, electra_box_item, nss_logger):
    """

    :param box_item:
    :param electra_box_item:
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return:
    """
    if not electra_box_item:
        id_match = search(r'^(\d{3,4}).*[Ss]cor.*[Ss]ummary.*\d{4}.*\.xlsx$', box_item.name)
        id_str = id_match.group(1)
        try:
            id_int = int(id_str)
        except ValueError as e:
            id_int = None
            logging.warning(e)
    else:
        id_match = search(r'^KG\d{6}_(\d{4})_Score_Summary_\d{4}.xlsx$', box_item.name)
        id_str = id_match.group(1)
        try:
            id_int = int(id_str)
        except ValueError as e:
            id_int = None
            nss_logger.warning(e)
    ummap_id = normalize_ummap_id(id_int)

    return ummap_id


def extract_redcap_event_name(dir_ummap_id, dir_visit_num, electra_dir_entry, electra_df):
    """

    :param dir_ummap_id:
    :param dir_visit_num:
    :param electra_dir_entry:
    :param electra_df:
    :return:
    """
    if not electra_dir_entry:
        redcap_event_name_str = f"visit_{dir_visit_num}_arm_1"
    else:
        ummap_visit_values = \
            electra_df[
                (electra_df.ptid == dir_ummap_id) &
                (electra_df.redcap_event_name == f"sv{dir_visit_num}_arm_1")
            ].ummap_visit_number.values
        if ummap_visit_values:
            ummap_visit_value = ummap_visit_values[0]
            redcap_event_name_str = f"visit_{ummap_visit_value}_arm_1"
        else:
            redcap_event_name_str = None

    return redcap_event_name_str


def local_build_accum_row(summ_sheet_df, parse_dict, dir_entry, electra_df, nss_logger):
    """
    Build record row for dataframe of records for eventual REDCap import

    :param summ_sheet_df:
    :param parse_dict:
    :param dir_entry: Spreadsheet DirEntry object
    :type dir_entry: os.DirEntry
    :param electra_df:
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return:
    """
    row_dict = {}
    for raw_field, spec_dict in parse_dict.items():
        row_idx, col_idx = return_col_row_of_val(summ_sheet_df, spec_dict['anchor'])
        if row_idx is not None:
            raw_value = summ_sheet_df.loc[row_idx + spec_dict['row_diff'], col_idx + spec_dict['col_diff']]
            if pd.isna(raw_value) or raw_value.strip().upper() in ["", "NA", "N/A"]:
                value = None
            else:
                value = \
                    convert_x_to_dtype(raw_value, spec_dict['dtype'], spec_dict['anchor'], dir_entry.path, nss_logger)
            row_dict[raw_field] = value

    electra_dir_entry = True if match(".*ELECTRA.*", dir_entry.path) else False
    dir_ummap_id = local_extract_dir_ummap_id(dir_entry, electra_dir_entry, nss_logger)
    dir_visit_num = local_extract_dir_visit_num(dir_entry, nss_logger)
    redcap_event_name = extract_redcap_event_name(dir_ummap_id, dir_visit_num, electra_dir_entry, electra_df)

    row_dict['redcap_event_name'] = redcap_event_name

    return row_dict


def box_build_accum_row(summ_sheet_df, parse_dict, box_item, electra_df, nss_logger):
    """
    Build record row for dataframe of records for eventual REDCap import

    :param summ_sheet_df:
    :param parse_dict:
    :param box_item:
    :param electra_df:
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return:
    """
    row_dict = {}
    for raw_field, spec_dict in parse_dict.items():
        row_idx, col_idx = return_col_row_of_val(summ_sheet_df, spec_dict['anchor'])
        if row_idx is not None:
            raw_value = summ_sheet_df.loc[row_idx + spec_dict['row_diff'], col_idx + spec_dict['col_diff']]
            if pd.isna(raw_value) or raw_value.strip().upper() in ["", "NA", "N/A"]:
                value = None
            else:
                value = \
                    convert_x_to_dtype(raw_value, spec_dict['dtype'], spec_dict['anchor'], box_item.id, nss_logger)
            row_dict[raw_field] = value

    electra_box_item = True if match(r'^KG\d{6}_\d{4}_Score_Summary_\d{4}.xlsx$', box_item.name) else False
    dir_ummap_id = box_extract_dir_ummap_id(box_item, electra_box_item, nss_logger)
    dir_visit_num = box_extract_dir_visit_num(box_item, nss_logger)
    redcap_event_name = extract_redcap_event_name(dir_ummap_id, dir_visit_num, electra_box_item, electra_df)

    row_dict['redcap_event_name'] = redcap_event_name

    return row_dict


def local_build_accum_df(dir_entries_list, parse_dict, electra_df, nss_logger):
    """
    Build dataframe of records for eventual REDCap import

    :param dir_entries_list:
    :param parse_dict:
    :param electra_df:
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return:
    """
    # build empty dataframe
    accum_df = pd.DataFrame(data=None, index=None, columns=parse_dict.keys())

    # loop over summary sheet DirEntries and process
    for dir_entry in dir_entries_list:
        print(f"  {dir_entry.name}")
        try:
            summ_sheet_df = pd.read_excel(dir_entry.path, sheet_name=0, header=None, dtype=str)
        except:
            summ_sheet_df = pd.DataFrame(data=None)
            nss_logger.warning(f"Cannot process \"{dir_entry.path}\"")
        if not summ_sheet_df.empty:
            row_dict = local_build_accum_row(summ_sheet_df, parse_dict, dir_entry, electra_df, nss_logger)
            accum_df = accum_df.append(row_dict, ignore_index=True)
            nss_logger.info(f"Processed \"{dir_entry.path}\"")

    return accum_df.dropna(axis="index", how="all")


def box_build_accum_df(box_items_list, parse_dict, electra_df, nss_logger):
    """
    Build dataframe of records for eventual REDCap import

    :param box_items_list:
    :param parse_dict:
    :param electra_df:
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :return:
    """
    # build empty dataframe
    accum_df = pd.DataFrame(data=None, index=None, columns=parse_dict.keys())

    # loop over summary sheet DirEntries and process
    for box_item in box_items_list:
        print(f"  {box_item.name}")
        try:
            summ_sheet_df = pd.read_excel(box_item.content(), sheet_name=0, header=None, dtype=str)
        except:
            summ_sheet_df = pd.DataFrame(data=None)
            nss_logger.warning(f"Cannot process {box_item.id} with name \"{box_item.name}\"")
        if not summ_sheet_df.empty:
            row_dict = box_build_accum_row(summ_sheet_df, parse_dict, box_item, electra_df, nss_logger)
            accum_df = accum_df.append(row_dict, ignore_index=True)
            nss_logger.info(f"Processed {box_item.id} with name \"{box_item.name}\"")

    return accum_df.dropna(axis="index", how="all")


def normalize_ummap_id(id_):
    """
    Normalize UMMAP IDs

    :param id_: UMMAP ID
    :type id_: str
    :return: normalized UMMAP ID
    :rtype: str
    """
    id_str = str(id_)
    if match(r'^UM\d{8}$', id_str):
        return id_str
    elif match(r'^\d{3,4}$', str(id_str)):
        return "UM" + "0" * (8 - len(id_str)) + id_str
    else:
        raise Exception(f"UMMAP ID {id_str} doesn't conform to expected form")


def add_prefix_to_fu_visits(df, cols, prefix):
    """

    :param df:
    :param cols:
    :param prefix:
    :return:
    """
    df = df.copy(deep=True)
    for col in cols:
        df[prefix + col] = df.loc[df.redcap_event_name != "visit_1_arm_1", col]
        df[col] = df.loc[df.redcap_event_name == "visit_1_arm_1", col]
    return df


def get_ivp_complete(df):
    """

    :param df:
    :return:
    """
    ivp_complete = (df.ivp_a1_complete == "2") & \
                   (df.ivp_a2_complete == "2") & \
                   (df.ivp_a3_complete == "2") & \
                   (df.ivp_a4_complete == "2") & \
                   (df.ivp_a5_complete == "2") & \
                   (df.ivp_b1_complete == "2") & \
                   (df.ivp_b4_complete == "2") & \
                   (df.ivp_b5_complete == "2") & \
                   (df.ivp_b6_complete == "2") & \
                   (df.ivp_b7_complete == "2") & \
                   (df.ivp_b8_complete == "2") & \
                   (df.ivp_b9_complete == "2") & \
                   (df.ivp_d1_complete == "2") & \
                   (df.ivp_d2_complete == "2")
    return ivp_complete


def get_fvp_complete(df):
    """

    :param df:
    :return:
    """
    fvp_complete = (df.fvp_a1_complete == "2") & \
                   (df.fvp_a2_complete == "2") & \
                   (df.fvp_a3_complete == "2") & \
                   (df.fvp_a4_complete == "2") & \
                   (df.fvp_b1_complete == "2") & \
                   (df.fvp_b4_complete == "2") & \
                   (df.fvp_b5_complete == "2") & \
                   (df.fvp_b6_complete == "2") & \
                   (df.fvp_b7_complete == "2") & \
                   (df.fvp_b8_complete == "2") & \
                   (df.fvp_b9_complete == "2") & \
                   (df.fvp_d1_complete == "2") & \
                   (df.fvp_d2_complete == "2")
    return fvp_complete


def retrieve_redcap_dataframe(redcap_api_uri, redcap_project_token, fields_raw, vp=True):
    """

    :param redcap_api_uri:
    :param redcap_project_token:
    :param fields_raw:
    :param vp:
    :return:
    """
    fields = ",".join(fields_raw)
    # get data
    request_dict = {
        'token': redcap_project_token,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'csvDelimiter': '',
        'fields': fields,
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    r = requests.post(redcap_api_uri, request_dict, verify=vp)
    df_raw = pd.DataFrame.from_dict(r.json())

    df_cln = df_raw[df_raw.ptid.str.match(r'^UM\d{8}$') &
                    (df_raw.redcap_event_name.str.match(r'^visit_\d+_arm_1$') |
                     df_raw.redcap_event_name.str.match(r'^sv\d+_arm_1$')) &
                    pd.notna(df_raw.form_date) &
                    (df_raw.form_date != "")]

    return df_cln


def import_redcap_data(redcap_api_uri, redcap_project_token, importable_csv_data, nss_logger, vp=True):
    """

    :param redcap_api_uri:
    :param redcap_project_token:
    :param importable_csv_data:
    :param nss_logger: Logger object for writing to app log
    :type nss_logger: logging.Logger
    :param vp:
    """
    request_dict = {
        'token': redcap_project_token,
        'content': 'record',
        'format': 'csv',
        'type': 'flat',
        'overwriteBehavior': 'normal',
        'data': importable_csv_data,
        'returnContent': 'count',
        'returnFormat': 'json'
    }
    request_result = requests.post(redcap_api_uri, request_dict, verify=vp)
    if request_result.status_code == 200:
        nss_logger.info(f"REDCap Import - Imported {request_result.json()['count']} records")
    elif request_result.status_code == 400:
        nss_logger.error(f"REDCap Error - {request_result.reason} - {request_result.json()['error']}")
    else:
        nss_logger.error(f"REDCap Error - {request_result.reason} - {request_result.content}")


########################
# Box Client Functions #

def get_box_authenticated_client(box_json_config_path):
    """
    Get an authenticated Box client for a JWT service account

    :param box_json_config_path:
    :return:
    """
    if not os.path.isfile(box_json_config_path):
        raise ValueError("`box_json_config_path` must be a path to the JSON config file for your Box JWT app")
    auth = JWTAuth.from_settings_file(box_json_config_path)
    auth.authenticate_instance()
    return Client(auth)


def get_box_subitems(box_client, box_folder, fields):
    """
    Get a collection of all immediate folder items

    :param box_client:
    :param box_folder:
    :param fields:
    """
    items = []
    # fetch folder items and add subfolders to list
    for item in box_client.folder(folder_id=box_folder['id']).get_items(fields=fields):
        items.append(item)
    return items
