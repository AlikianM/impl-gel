'''
cipapi_download_html_reports

Polling GeL CIPAP and downloading all GMC relevant html reports.
These are archived and relevant tables possibly scraped as needed.

Created 07-12-2018 by Graham Rose
'''

from poll_cipapi import CipApi
from cipauth import Authenticate
import os

# credientials stored as environment vars
CIP_API_SERVER_URL = os.getenv("cip_api_server_url")

#ir = CipApi(sample_type='cancer', search='imperial', download_html=False)
ir = CipApi(sample_type='raredisease', search='imperial', download_html=False)
rd_cases = ir.all_relevant_cases

# itererate through cases
for rd_case in rd_cases:

    auth = Authenticate()

    # access rd case id and versions
    rd_case = rd_case['interpretation_request_id']
    ir_id = rd_case.split('-')[0]
    ir_version = rd_case.split('-')[1]

    # new endpoint for each case payload
    interpretation_request_case_url = CIP_API_SERVER_URL.format(
        endpoint='interpretation-request/{ir_id}/{ir_version}/')

    interpretation_request_case_url = interpretation_request_case_url.format(
        ir_id=ir_id, ir_version=ir_version)

    payload=""

    interpretation_request_case_data = auth.get_url_json_response(
        url=interpretation_request_case_url, payload=payload)

    # strip ir data from the metadata and reports in case data
    interpretation_request_payload = interpretation_request_case_data[
        'interpretation_request_data']['json_request']

    try:
        # output specificDisease per ir_id
        pedigree = interpretation_request_payload['pedigree']
        participants = pedigree['participants']

        for item in participants:
            disorderList = item['disorderList']
            for item in disorderList:
                specificDisease = item['specificDisease']
                print("ir_id=", ir_id, "ir_version=", ir_version,
                        "specificDisease=", specificDisease, sep='\t')
    except:
        pass
