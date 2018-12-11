'''
cipapi_cancer_download_html_reports.py

Script uses GeL CIPAPI, authenticates and searches for X disease VCF files.
These are downloaded to relevant space.

Created 19-09-2018 by Graham Rose
'''

import requests
import os
import csv
import json as json
import urllib.request
import cipauth as auth  # handle auth tokens

'''# Credientials stored as environment vars
CIP_API_USERNAME = os.getenv("CIP_API_USERNAME")
CIP_API_PASSWORD = os.getenv("CIP_API_PASSWORD")'''
CIP_API_SERVER_URL = os.getenv("CIP_API_SERVER_URL")

# IO paths
current_dir_path = os.path.dirname(os.path.realpath('__file__'))
#html_dir = 'ir_html_files' # Output folder
gmc = 'imperial' # Limit to local cases
disease = 'raredisease'

class run_api_call():

    # Return raw response stream - for downloading files endpoint. Raw socket response content expected.
    def get_url_file_download(self, url):
        #print("debug: running function get_url_json_download")

        response = requests.get(
            url=url, headers=auth.get_authenticated_header(), stream = True)

        return response


    # Get all data from interpretation-request
    def get_gel_ir_summary(self):
        #print("debug: running function get_gel_ir")

        print("#Searching ", gmc, " GMC for all ", disease, "cases")

        #  Note trailing forward slash in endpoint was appending params using requests
        url = CIP_API_SERVER_URL
        auth_endpoint = "interpretation-request"

        interpretation_request_url = url.format(
            endpoint=auth_endpoint)

        # Run request through auth and return json, passing params literals
        interpretation_request = auth.get_url_json_response(
            url=interpretation_request_url + '?page_size=1000&sample_type=raredisease&search=imperial&last_status=sent_to_gmcs')

        number_results = len(interpretation_request['results'])
        print("{} {}".format(
            "#Found imperial cases within cipapi with status=sent_to_gmcs:", number_results))

        
        # List all ir_id and ir_version, add to dict and pass to ir case url
        results_counter = 0
        allcases = {}
        while results_counter < number_results:
            ir_id, ir_version = map(
                str, interpretation_request['results'][results_counter]['interpretation_request_id'].split('-'))
            
            #print("found case...", ir_id, ir_version)


            allcases.update(ir_id=ir_version)
            self.get_gel_ir_case_data(ir_id, ir_version)

            results_counter += 1

        self.get_gel_ir_case_data(ir_id, ir_version)


    # Itererate through cases, extract attributes
    def get_gel_ir_case_data(self, ir_id, ir_version):
        #print("debug running: get_gel_ir_case_data")

        # New endpoint
        interpretation_request_case_url = CIP_API_SERVER_URL.format(
            endpoint='interpretation-request/{ir_id}/{ir_version}/')

        interpretation_request_case_url = interpretation_request_case_url.format(
            ir_id=ir_id, ir_version=ir_version)

        interpretation_request_case_data = auth.get_url_json_response(
            url=interpretation_request_case_url)

        # Strip ir data from the metadata and reports in case data
        interpretation_request_payload = interpretation_request_case_data['interpretation_request_data']['json_request']

        try:
            # Output specificDisease per ir_id
            pedigree = interpretation_request_payload['pedigree']
            participants = pedigree['participants']

            for item in participants:
                disorderList = item['disorderList']
                for item in disorderList:
                    specificDisease = item['specificDisease']
                    print("#ir_id=", ir_id, "ir_version=", ir_version,
                          "specificDisease=", specificDisease, sep=",")
        except:
            pass


    # Parse json data payload
    def parse_gel_ir_case_data_json(self, json_output):

        print("debug running: parse_gel_ir_case_data_summary_json")

        # Manual parsing key vars
        interpretation_request_payload = json_output['interpretation_request_data']['json_request']
        pedigree = interpretation_request_payload['pedigree']
        tieredVariants = interpretation_request_payload['TieredVariants']
        interpreted_genome = json_output['interpreted_genome'][-1]
        clinical_report = json_output['clinical_report'][-1]


if __name__ == '__main__':
    start=run_api_call()
    start.get_gel_ir_summary()
    print("Done")
