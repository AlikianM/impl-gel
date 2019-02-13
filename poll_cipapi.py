import requests
import os
import csv
import json as json
import urllib.request
from clint.textui import progress

from cipauth import Authenticate


class CipApi():
    """
    Polling GeLs CIP API. Wraps around utilised api methods. 
    Creation of authentication token handled by separate Authentication module.
    """
    
    def __init__(self, sample_type, search, download_html=None):
        """
        Initialises a CipApi instance with credentials and api url taken as environment variables.
        
        Parameters -
            sample_type: cancer or raredisease
            search term: eg. specify ldp
            download_html: boolean, cancer relevant only 
        """
         
        print("Initialising PollCipApi")
        
        if sample_type == "cancer" or sample_type == "raredisease":
            pass
        else:
            raise ValueError('{sample_type} is not a valid entry for "sample_type".',
                             'Enter "raredisease" or "cancer".'.format(sample_type=sample_type))
         
        if search == "imperial":
            pass
        else:
            raise ValueError('{gmc} is not a valid entry for "search"; enter "imperial".'.format(search=search))
        

        self.sample_type = sample_type
        self.search = search
        self.cipapi_url = os.getenv("cip_api_server_url")

        # all_case_html_reports list unpopulated if not sample_type cancer 
        self.all_cases, self.all_case_html_reports = self.get_all_ir_cases()
        self.all_relevant_cases = self.get_all_relevant_ir_cases()
        
        if download_html == True and self.sample_type == "cancer":
            self.download_cancer_html_reports()
        else:
            pass
        
        
    def get_all_ir_cases_basic(self):
        """
        Basic poll. Overview of all the cases. Results are displayed in pages of 100 documents. 
        All results will get a previous and next field with the url to the previous and next page. f
        First and last will have a null value in previous and next respectively.
        """
        endpoint = "interpretation-request" # no forward / as passing params as payload
        payload = {'page_size': '10', 'sample_type': self.sample_type, 'search': self.search}
        
        url = self.cipapi_url.format(
            endpoint=endpoint)
        
        auth = Authenticate()
        interpretation_request = auth.get_url_json_response(url, payload)
        
        return interpretation_request

    
    def get_all_ir_cases(self):
        """
        Look for all ir cases, return cases, download case html reports if sample_type is cancer"
        """
        print("Searching ", self.search, " LDP within WL GMC for all ", self.sample_type, "cases...")

        endpoint = "interpretation-request" # no forward / as passing params as payload

        url = self.cipapi_url.format(
            endpoint=endpoint)

        auth = Authenticate()
        
        # cycle through all pages, defualts to 100 case per page
        all_cases = []
        all_cases_html_reports = []
        
        last_page = False
        page = 1
        while not last_page:
            payload = {'page': page, 'sample_type': self.sample_type, 'search': self.search}
            
            interpretation_request = auth.get_url_json_response(url, payload)
            
            request_list_results = interpretation_request["results"]
            
            # ignore blocked cases
            for result in request_list_results:
                if result["last_status"] != "blocked":
                    if self.sample_type == "raredisease":
                        all_cases.append({
                                "interpretation_request_id": result["interpretation_request_id"],
                                "sample_type": result["sample_type"],
                                "last_status": result["last_status"]})
                    
                    # grab all cancer html urls as well
                    elif self.sample_type == "cancer":
                        all_cases.append({
                                "interpretation_request_id": result["interpretation_request_id"],
                                "sample_type": result["sample_type"],
                                "last_status": result["last_status"]})
                        for files in result['files']:
                            ir_file_type = files["file_type"]
                            if ir_file_type == "html":
                                all_cases_html_reports.append({
                                    "interpretation_request_id": result["interpretation_request_id"],
                                    "sample_type": result["sample_type"],
                                    "report_html_url": files["url"],
                                    "report_html_filename" : files["file_name"]})
            
            if interpretation_request["next"]:
                page += 1
            else:
                last_page = True
        
        print("{} {} {} {}".format("Cases for", self.sample_type, "found: ", interpretation_request["count"]))
                
        return all_cases, all_cases_html_reports
    
    
    
    def get_all_relevant_ir_cases(self):
        """
        Return sublist of only those ir cases that are of interest,
        eg. 'sent_to_gmcs', 'report_generated'
        """
        print("Striping out all non relevant cases, ie. cases not sent to gmc...")
        
        status_types_to_keep = {
            "cancer": ["interpretation_generated", "sent_to_gmcs", "report_generated", "report_sent"],
            "raredisease": [ "sent_to_gmcs", "report_generated", "report_sent"]}
        
        case_list_to_poll = [
            case for case in self.all_cases if case["last_status"] in status_types_to_keep[self.sample_type]]
              
        print("{} {} {} {}".format("Relevant cases for", self.sample_type, "kept: ", len(case_list_to_poll)))
        
        return case_list_to_poll
    
            
    def download_cancer_html_reports(self):
        """
        Download all cancer GeL static html reports for each case. 
        Applies only for cancer sample_types.
        """
        
        print("Setting up folder structure...")
        current_dir_path = os.path.dirname(os.path.realpath('__file__'))
        html_dir = 'ir_html_files'
        
        for case in self.all_relevant_cases:
            print("Pulling html for case...", case["interpretation_request_id"])

            for case_html_report in self.all_case_html_reports:
                if case_html_report["interpretation_request_id"] == case["interpretation_request_id"]:
                    result_filename = case_html_report["report_html_filename"]
                    result_report_url = case_html_report["report_html_url"]

                    file_output_path = os.path.join(current_dir_path, html_dir, result_filename)

                    # if file do not already exist in output dir, pass to download function
                    if os.path.exists(file_output_path) == False:
                        download = self.return_response_as_stream(
                            result_report_url)
                        
                        with open(file_output_path, 'wb') as f:
                            print("{} {}".format("Downloading the report...", result_filename))
                            total_length = int(download.headers.get('content-length'))
                            
                            for chunk in progress.bar(download.iter_content(chunk_size=512), expected_size=(total_length/512)+1):
                                if(chunk):
                                    f.write(chunk)
                                    f.flush()

                    elif os.path.exists(file_output_path) == True:
                        print("{} {}".format("File already exits locally, skipping download...", result_report_url))
           

    # Return raw response stream - for downloading files endpoint. Raw socket response content expected.
    def return_response_as_stream(self, result_report_url):
        """
        Return raw response stream
        """
        auth = Authenticate()
        
        response = requests.get(
            url=result_report_url, headers=auth.get_authenticated_header(), stream = True)

        return response
