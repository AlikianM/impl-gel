'''
cipapi_download_html_reports

Polling GeL CIPAP and downloading all GMC relevant html reports.
These are archived and relevant tables possibly scraped as needed.

Created 07-12-2018 by Graham Rose
'''

from poll_cipapi import CipApi
from cipauth import Authenticate

#ir = CipApi(sample_type='cancer', search='imperial', download_html=False)
ir = CipApi(sample_type='cancer', search='imperial', download_html=True)
