"""
cipapi_cancer_download_html_reports.py

Script uses GeL CIPAPI, authenticates and downloads all GMC cancer html reports.
These are archived and relevant tables can be scraped as required later.

Created 10-09-2018 by Graham Rose
"""


import requests
import os
import csv
import json as json
import urllib.request
import pandas as pd
from tabulate import tabulate

# Credientials stored as environment vars
CIP_API_USERNAME = os.getenv("CIP_API_USERNAME")
CIP_API_PASSWORD = os.getenv("CIP_API_PASSWORD")
CIP_API_SERVER_URL = os.getenv("CIP_API_SERVER_URL")

# IO paths setup
current_dir_path = os.path.dirname(os.path.realpath('__file__'))
html_dir = 'ir_html_files' # Output html folder
ir_html_scraped_files = 'ir_html_scraped_files'  # output soup folder
gmc = 'imperial' # Limit to local cases
disease = 'cancer'

class run_api_call():
    """
    Retreive the json file from interpretation-request, passing request
    through authentication and response modules.
    """

    # Authenticate
    def get_authenticated_header(self):
        #print("debug: running function get_authenticated_header")

        url = CIP_API_SERVER_URL
        auth_endpoint = "get-token/"
        
        url_response = requests.post(
            url=url.format(endpoint=auth_endpoint),
            json=dict(
                username=CIP_API_USERNAME,
                password=CIP_API_PASSWORD,
            ),
        )
        url_response_json = url_response.json()
        token = url_response_json.get('token')

        auth_header = {
            'Accept':    'application/json',
            "Authorization": "JWT {token}".format(token=token),
        }
        return auth_header

    # Return json response if successful
    def get_url_json_response(self, url):
        #print("debug: running function get_url_json_response")

        # Pass the parameters as single payload
        payload = {'page_size':'1000','sample_type':disease,'search':gmc}

        # Should be: https://cipapi.genomicsengland.nhs.uk/api/2/interpretation-request?page_size=1000&sample_type=cancer&search=imperial
        response = requests.get(
            url=url, headers=self.get_authenticated_header(), params=payload)

        if response.status_code != 200:
            raise ValueError(
                "Received status: {status} for url: {url} with response: {response}".format(
                    status=response.status_code, url=url, response=response.content)
            )
        return response.json()


    # Return raw response stream - for downloading files endpoint. Raw socket response content expected.
    def get_url_file_download(self, url):
        #print("debug: running function get_url_json_download")

        response = requests.get(
            url=url, headers=self.get_authenticated_header(), stream = True)

        return response


    """Retreive the json file from interpretation-request endpoint,
    where we only are interested in the file paths present and and update on the status
    of the samples """
    def get_gel_ir_summary(self):
        #print("debug: running function get_gel_ir")

        print("Searching ", gmc, " GMC for all ", disease, "cases")

        #  Note previous trailing forward slash in endpoint,
        #  before using has appending params using requests

        url = CIP_API_SERVER_URL
        auth_endpoint = "interpretation-request" 

        interpretation_request_url = url.format(
            endpoint=auth_endpoint)

        interpretation_request = self.get_url_json_response(
            url=interpretation_request_url)

        #print(interpretation_request.keys())

        print("{} {}".format("Found imperial cases within cipapi:", len(interpretation_request['results'])))


        # Iterate json to access url of html files for all results
        rows_list = []
        samples_info_df = pd.DataFrame({})

        for results in interpretation_request['results']:

            # Used for a samples status table, appending to list of dictionarys
            case_id = results['case_id']
            cancer_participant = results['cancer_participant']
            last_status = results['last_status']
            interpretation_request_id = results['interpretation_request_id']
            
            row_dict = {'case_id': case_id, 'cancer_participant': cancer_participant, 'last_status': last_status, 'interpretation_request_id': interpretation_request_id}
            rows_list.append(row_dict)

            # Looking for only filenames within the results
            # section of the json file returned. Want to download the html files only.

            for files in results['files']:
                ir_report_url = files.get('url')
                ir_filename = files.get('file_name')
                ir_file_type = files.get('file_type')

                # Save only html files to file and not any csv filenames that may be present

                if ir_file_type == "html":
                    print("{} {}".format("Found html...", ir_filename))

                    file_output_path = os.path.join(
                        current_dir_path, html_dir, ir_filename)

                    # Check if file already exists in the output dir, skip downloading again
                    if os.path.exists(file_output_path) == False:
                        self.download_report(ir_filename, ir_report_url)

                    elif os.path.exists(file_output_path) == True:
                        print("{} {}".format("This html already exists in local folder, skipping download...", ir_report_url))

       
        samples_info_df = pd.DataFrame(rows_list)
        #print(tabulate(samples_info_df, headers='keys', tablefmt='psql'))
        
        # Output as csv
        file_output_path = os.path.join(
            current_dir_path, ir_html_scraped_files, 'samples_info.csv')

        with open(file_output_path, 'w') as f:
            f.write(samples_info_df.to_csv(index=False))


    """ Download all html files. Use alternative reponse without handling errors as an empty (not json) 
    response body is expected from this endpoint"""
    def download_report(self, ir_filename, ir_report_url):

        # The url is already provided from within ir endpoint, so pass this variable.
        # Run request through authentication and return json
        file_download = self.get_url_file_download(url=ir_report_url)

        file_output_path = os.path.join(current_dir_path, html_dir, ir_filename)

        # Write to output
        with open(file_output_path, 'wb') as f:
            print("{} {}".format("Downloading html...", ir_filename))

            for chunk in file_download.iter_content(chunk_size=128):
                f.write(chunk)
            
            print("{} {}{}".format("Downloading html...", ir_filename, "...finished"))
            

if __name__ == '__main__':
    start=run_api_call()
    start.get_gel_ir_summary()
    print("Done")
