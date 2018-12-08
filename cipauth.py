'''
Authentication module for GeL CIPAPI user

Created 12-09-2018 by Graham Rose
'''

import requests
import os

class Authenticate():
    """
    Authenticate API call, return response if successful
    """
    
    def __init__(self):
        # As environment vars
        self.cipapi_url = os.getenv("CIP_API_SERVER_URL")
    
    def get_authenticated_header(self):
        """
        Get a CIP-API token, then creates Accept/Auth headers.
        """
        
        #print("Getting CIPAPI headers...")

        auth_endpoint = "get-token/"

        url_response = requests.post(
            url=self.cipapi_url.format(endpoint=auth_endpoint),
            json=dict(
                username=os.getenv("CIP_API_USERNAME"),
                password=os.getenv("CIP_API_PASSWORD"),
            ),
        )
        url_response_json = url_response.json()
        token = url_response_json.get('token')

        auth_header = {
            "Accept": "application/json",
            "Authorization": "JWT {token}".format(token=token),
        }
        return auth_header

    def get_url_json_response(self, url, payload):
        """
        Return url response as json, if sucessful
        """
            
        response = requests.get(
            url=url, headers=self.get_authenticated_header(), params=payload)


        if response.status_code != 200:
            raise ValueError(
                "Received status: {status} for url: {url} with response: {response}".format(
                    status=response.status_code, url=url, response=response.content)
            )
        return response.json()
    
    
    
