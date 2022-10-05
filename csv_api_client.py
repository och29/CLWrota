#!/usr/bin/env python3

"""
MIT License

Copyright (c) 2022 Rotamap Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

##############################################################################

Version 0.0.3 : 20 June 2022

##############################################################################

This is an example API client to extract rota data from one or more instances
of Rotamap's Public API service and export it to a CSV file.

Please view README.md for instructions on how to use the client.

##############################################################################
"""

import argparse
import sys
import csv
import json
import os.path
from datetime import date, timedelta, datetime
import requests


def load_settings(settings_path, tokens_path, csv_path, verbose, iso_dates):

    """
    Loads settings.json file.
    """

    settings_file_path = os.path.join(settings_path or '', 'settings.json')

    try:
        with open(settings_file_path, 'r') as f:
            settings = json.load(f)
    except FileNotFoundError as error:
        error_quit(str(error))
    except json.decoder.JSONDecodeError as error:
        error_quit(str(error))

    validate_settings(settings, tokens_path, csv_path, verbose, iso_dates)


def validate_settings(settings, tokens_path, csv_path, verbose, iso_dates):

    """
    Validates the values chosen in the settings file.
    """

    if 'additional_fields' not in settings:
        settings['additional_fields'] = {}

    if 'required_fields' not in settings:
        settings['required_fields'] = []

    # Check systems are of the valid choices
    systems = ('clwrota', 'medirota')
    for department in settings['departments']:
        if department['system'] not in systems:
            error_quit('System must be one of: ' + str(systems))

    # Check additional fields are present in the return data set
    if not all(
        fields in settings['return_data_set'].keys()
        for fields in settings['additional_fields'].keys()
    ):
        error_quit('All additional_fields keys must be in return_data_set')

    # Check the number of days reporting period is an integer
    try:
        int(settings['day_range'])
    except ValueError:
        error_quit('Ensure day_range is an integer')

    load_tokens(settings, tokens_path, systems, csv_path, verbose, iso_dates)


def load_tokens(settings, tokens_path, systems, csv_path, verbose, iso_dates):

    """
    Loads tokens.json file.
    """

    tokens_file_path = os.path.join(tokens_path or '', 'tokens.json')

    # Load tokens file
    try:
        with open(tokens_file_path, 'r') as f:
            tokens_data = json.load(f)
    except FileNotFoundError:
        # Construct tokens dictionary
        tokens_data = {}
        for system in systems:
            tokens_data[system] = {
                department['shortname']: None
                for department in settings['departments']
                if department['system'] == system
            }

    except json.decoder.JSONDecodeError as error:
        error_quit(str(error))

    authenticate(settings, tokens_data, tokens_file_path, csv_path, verbose, iso_dates)


def authenticate(settings, tokens_data, tokens_file_path, csv_path, verbose, iso_dates):

    """
    Provides authentication to one or more instances of the Public API.
    """

    department_urls = []

    # Iterate through each department described in the settings file
    for department in settings['departments']:

        shortname = department['shortname']
        system = department['system']

        # Define base Public API url
        base_url = 'https://{}.{}.com/publicapi/'.format(
            shortname,
            system
        )

        if verbose:
            sys.stderr.write('Getting token for ' + system + ' ' + shortname + '\n')

        # Get department token from tokens data
        try:
            token = tokens_data[system][shortname]

            # Access Public API landing page with token
            url = '{}{}/landing/'.format(
                base_url,
                token
            )

            response = requests.get(url)
            valid_token = response.status_code in (200, 420)

        except KeyError:
            valid_token = False

        except requests.ConnectionError as error:
            error_quit(str(error))

        if not valid_token:
            if verbose:
                sys.stderr.write('New token required for ' + system + ' ' + shortname + '\n')

            url = base_url + 'login/'

            try:
                # Login to the Public API to get a new token
                response = requests.post(
                    url,
                    data={'username': department['auth']['username'],
                          'password': department['auth']['password']},
                    headers={'Accept': 'application/json'}
                )
                response.raise_for_status()

                token = response.json()['token']

                if verbose:
                    sys.stderr.write('Saving token for ' + system + ' ' + shortname + '\n')

                # Save token to tokens.json file
                tokens_data[system][shortname] = token

                with open(tokens_file_path, 'w') as tokens_file:
                    json.dump(
                        tokens_data,
                        tokens_file,
                        ensure_ascii=False,
                        indent=4
                    )

            except requests.ConnectionError as error:
                error_quit(str(error))

            except requests.HTTPError:
                error_quit(str(response.json()['error']['message']))

        department_urls.append(base_url + token)

    get_rota_data(settings, department_urls, csv_path, verbose, iso_dates)


def get_rota_data(settings, department_urls, csv_path, verbose, iso_dates):

    """
    Gathers data from one or more Public API instances and compiles the data
    into a single object.
    """

    all_rota_data = []

    from_date = date.today()
    to_date = from_date + timedelta(days=(int(settings['day_range']) - 1))

    # Iterate through PublicAPI urls
    for department_url in department_urls:

        url = '{}/person_rota/'.format(
            department_url
        )

        parameters = {'from_date': from_date,
                      'to_date': to_date}

        try:
            # Make request to get rota data
            response = requests.get(
                url,
                parameters,
                headers={'Accept': 'application/json'}
            )

            if verbose:
                sys.stderr.write('Getting data from ' + response.url + '\n')

            response.raise_for_status()

        except requests.ConnectionError as error:
            error_quit(str(error))

        except requests.HTTPError:
            error_quit(str(response.json()['error']['message']))

        department_rota_data = response.json()['person_rota']

        # Add to compiled rota data
        all_rota_data.extend(department_rota_data)

    process_data(settings, all_rota_data, csv_path, verbose, iso_dates)


def process_data(settings, all_rota_data, csv_path, verbose, iso_dates):

    """
    Applies processing to rota data.

    It will add any additional fields, only include rows of data with values
    for all keys included in the required_fields object, and will exclude any
    data attributes not included in return_data_set object in the settings file
    and rename and reorder the columns accordingly.
    """

    additional_fields = settings['additional_fields']
    required_fields = settings['required_fields']
    return_data_set = settings['return_data_set']
    processed_data = []

    # Iterate through the entire person rota data set
    for row in all_rota_data:

        # Add the additional field keys and values
        row.update(additional_fields)

        # Include rows that have data for all of the required fields
        # (if no required fields, all data is returned)
        include_row = all([str(value) for key, value in row.items() if key in required_fields])
        if include_row:
            item = {}
            # Iterate through the return_data_set keys and values
            for key, value in return_data_set.items():

                # If not using international date format, reformat ISO dates and timestamps
                if not iso_dates:
                    if key == 'date':
                        row[key] = datetime.fromisoformat(row[key]).strftime('%d/%m/%Y')
                    elif key == 'modified':
                        row[key] = datetime.fromisoformat(row[key]).strftime('%d/%m/%Y %H:%M:%S')

                try:
                    # Update person rota data set keys to the value defined in
                    # the return data set
                    item.update({value: row[key]})
                except KeyError as error:
                    error_quit('Unknown key in return_data_set: ' + str(error))

            processed_data.append(item)

    write_csv(return_data_set.values(), processed_data, csv_path, verbose)


def write_csv(column_headers, processed_data, csv_path, verbose):

    """
    Generates a CSV data output.

    If a directory is provided with the --csv argument then it will generate a
    CSV file titled with the current timestamp to that directory, otherwise the
    output will be provided to the stdout.
    """

    # If csv output provided, write csv file
    if csv_path:

        csv_file = 'output_{}.csv'.format(
            datetime.now().strftime("%H%M%S")
        )

        csv_file_path = os.path.join(csv_path, csv_file)

        if verbose:
            sys.stderr.write('Writing CSV to ' + csv_file_path + '\n')

        try:
            # Write a CSV file to the defined output path
            with open(csv_file_path, 'w', newline='') as output_file:
                writer = csv.DictWriter(
                    output_file,
                    fieldnames=column_headers
                )
                writer.writeheader()
                for row in processed_data:
                    writer.writerow(row)

        except (csv.Error, ValueError) as error:
            error_quit(str(error))

    # If no csv output specified, use stdout as output
    else:

        if verbose:
            sys.stderr.write('Writing CSV to stdout\n')

        try:
            writer = csv.DictWriter(
                sys.stdout,
                fieldnames=column_headers
            )
            writer.writeheader()
            for row in processed_data:
                writer.writerow(row)

        except (csv.Error, ValueError) as error:
            error_quit(str(error))

    if verbose:
        sys.stderr.write('Export complete!\n')


def error_quit(message):

    """
    Displays an error message and exits the client.
    """

    sys.stderr.write(message + '\n')
    sys.exit(1)


if __name__ == '__main__':

    """
    Loads and validates settings file. If a tokens file is present it will be
    loaded, if not the data will be created from the settings file.
    """

    description = """This is an example API client to extract rota data from
                     one or more instances of Rotamap's Public API service and
                     export it to a CSV file. Please view README.md for
                     instructions on how to use the client."""

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help="Print progress."
    )
    parser.add_argument(
        '-i',
        '--iso_dates',
        action='store_true',
        help="Use international date format for date values."
    )
    parser.add_argument(
        '-c',
        '--csv',
        help="Path to directory to create CSV output file. Output written to stdout if omitted."
    )
    parser.add_argument(
        '-t',
        '--tokens',
        help="Path to directory to use for tokens file. Defaults to project root directory."
    )
    parser.add_argument(
        '-s',
        '--settings',
        help="Path to directory where settings.json file is. Defaults to project root directory."
    )

    args = parser.parse_args()

    load_settings(args.settings, args.tokens, args.csv, args.verbose, args.iso_dates)
