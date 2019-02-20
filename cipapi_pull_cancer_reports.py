'''
cipapi_download_html_reports

Polling GeL CIPAP and downloading all GMC relevant html reports.
These are archived and relevant tables possibly scraped as needed.

Created 07-12-2018 by Graham Rose
'''

from poll_cipapi import CipApi
from cipauth import Authenticate
from cipapi_cancer_scrape_html_reports import *

#ir = CipApi(sample_type='cancer', search='imperial', download_html=False)
ir = CipApi(sample_type='cancer', search='imperial', download_html=True)

# scrape htmls, save tables to concatenated csv files
parse_cancer_htmls()
