import errno

import bz2
import json
import os
import logging

# Returns the list of market files path
# Directory structure should look like
# data_path: PRO/2020/Aug/2/29936590/1.171755571.bz2
# bz2 files are automatically decompressed when parsed the first time
def parse_market_files(data_path, market_definition_filters={}, include_years=None, include_months=None, include_days=None, delete_files=False):
    files = []

    # Iterate over sub directories year/month/day/event/market
    years = [dI for dI in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, dI))]
    if include_years:
        years = list(set(years) & set(include_years))
    for year in sorted(years):
        year_path = os.path.join(data_path, year)
        months = [dI for dI in os.listdir(year_path) if os.path.isdir(os.path.join(year_path, dI))]
        if include_months:
            months = list(set(months) & set(include_months))
        for month in sorted(months):
            month_path = os.path.join(year_path, month)
            days = [dI for dI in os.listdir(month_path) if os.path.isdir(os.path.join(month_path, dI))]
            if include_days:
                days = list(set(days) & set(include_days))
            for day in sorted(days):
                day_path = os.path.join(month_path, day)
                event_ids = [dI for dI in os.listdir(day_path) if os.path.isdir(os.path.join(day_path, dI))]
                for event_id in event_ids:
                    event_path = os.path.join(day_path, event_id)
                    markets_files = [dI for dI in os.listdir(event_path) if dI.endswith('.bz2')]
                    for market_file in markets_files:
                        market_file_compressed = os.path.join(event_path, market_file)
                        market_file = market_file_compressed[:-4]  # assuming the filepath ends with .bz2

                        # Decompress bz file if target directory does not exist
                        if not os.path.isfile(market_file):
                            logging.info(f'decompressing market file {market_file_compressed}')
                            zipfile = bz2.BZ2File(market_file_compressed)
                            data = zipfile.read()
                            open(market_file, 'wb').write(data)

                        # Filter based on market definition
                        if _filter_market_file(market_file, market_definition_filters):
                            if delete_files:
                                logging.info(f'deleting market file {market_file_compressed}')
                                if os.path.isfile(market_file):
                                    os.remove(market_file)
                                if os.path.isfile(market_file_compressed):
                                    os.remove(market_file_compressed)
                            continue

                        files.append(market_file)

    if len(files) == 0:
        raise FileNotFoundError(errno.ENOENT, "No files found for passed filters", market_definition_filters)

    return files


# Filter based on market definition
# eg: market_definition_filters={'bettingType': 'ODDS', 'marketType': 'MATCH_ODDS'}
def _filter_market_file(market_file, market_definition_filters):
    if bool(market_definition_filters):
        with open(market_file) as f:
            first_line = f.readline()
            data = json.loads(first_line)
            market_definition = data['mc'][0]['marketDefinition']
            
            # Ignore Harness
            if "Trot" in market_definition['name'] or "Pace" in market_definition['name']:
                return True

            # Apply filters
            for k,v in market_definition_filters.items():
                if market_definition[k] != v:
                    return True

    return False
