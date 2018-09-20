'''
Authentication module for GeL CIPAPI user

Created 12-09-2018 by Graham Rose
'''

import requests
import os

# As environment vars
CIP_API_USERNAME = os.getenv("CIP_API_USERNAME")
CIP_API_PASSWORD = os.getenv("CIP_API_PASSWORD")
CIP_API_SERVER_URL = os.getenv("CIP_API_SERVER_URL")

# Authenticate
def get_authenticated_header():
    #print("debug running: get_authenticated_header")

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
def get_url_json_response(url):
    #print("debug running: get_url_json_response")

    response = requests.get(
        url=url, headers=get_authenticated_header())

    if response.status_code != 200:
        raise ValueError(
            "Received status: {status} for url: {url} with response: {response}".format(
                status=response.status_code, url=url, response=response.content)
        )
    return response.json()
