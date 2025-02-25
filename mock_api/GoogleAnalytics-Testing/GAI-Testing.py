"""A simple example of how to access the Google Analytics API."""

import argparse

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

import operator

# Configurations/Keys for Google Analytics
VIEW_ID = 'ga:121580132'
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')

def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
    """Get a service that communicates to a Google API.

  Args:
    api_name: The name of the api to connect to.
    api_version: The api version to connect to.
    scope: A list auth scopes to authorize for the application.
    key_file_location: The path to a valid service account p12 key file.
    service_account_email: The service account email address.

  Returns:
    A service that is connected to the specified API.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scope)

    http = credentials.authorize(httplib2.Http())

    # Build the service object.
    service = build(api_name, api_version, http=http, discoveryServiceUrl=DISCOVERY_URI)

    return service

def setup_and_get_service():
    # Define the auth scopes to request.
  scope = ['https://www.googleapis.com/auth/analytics.readonly']

  # Use the developer console and replace the values with your
  # service account email and relative location of your key file.
  service_account_email = 'aquaint-service-account@aquaint-analytics.iam.gserviceaccount.com'
  key_file_location = 'Aquaint-Analytics-acba58fc01fc.json'

  # Authenticate and construct service.
  return get_service('analytics', 'v4', scope, key_file_location,
                     service_account_email)

def retrieve_pageview_report(service, webpage_url):
    response = service.reports().batchGet(
        body={
            'reportRequests' : [
                {
                    # On the format of fields ulocations_dictionary[key] = sed in Query Explorer, see:
                    # https://developers.google.com/analytics/devguides/reporting/core/v4/samples
                    # https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
                    'viewId': VIEW_ID,
                    'dateRanges': [{'startDate' : '365daysAgo', 'endDate' : 'today'}],
                    'metrics': [{'expression': 'ga:sessions'}],
                    'dimensions': [{'name': 'ga:pagePath'}],
                    'filtersExpression': ('ga:pagePath==' + webpage_url),
                    #'quotaUser' : 'austin'
                }],
        }, quotaUser='austin'
    ).execute()

    print response
    #return parse_response_first_val(response)

# def parse_response_first_val(response):
#     # Parses and prints the Analytics Reporting API V4 response
#     # Here, we return the first value in the response, as only one value is expected
#   for report in response.get('reports', []):
#       columnHeader = report.get('columnHeader', {})
#       dimensionHeaders = columnHeader.get('dimensions', [])
#       metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
#       rows = report.get('data', {}).get('rows', [])

#     for row in rows:
#         dimensions = row.get('dimensions', [])
#         dateRangeValues = row.get('metrics', [])
#       for header, dimension in zip(dimensionHeaders, dimensions):
#           print header + ': ' + dimension + ';',
#         for i, values in enumerate(dateRangeValues):
#             #print '--Date range (' + str(i) + ')'
#           for metricHeader, value in zip(metricHeaders, values.get('values')):
#               print metricHeader.get('name') + ': ' + value
#             return str(value)

#   # zero is returned if response is empty
#   return str(0)


# def parse_response_all_vals_sum(response):
#     # Parses and prints the Analytics Reporting API V4 response
#     # Here, we return the sum of all values in the response
#   for report in response.get('reports', []):
#       columnHeader = report.get('columnHeader', {})
#       dimensionHeaders = columnHeader.get('dimensions', [])
#       metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
#       rows = report.get('data', {}).get('rows', [])

#     valueSum = 0
#     for row in rows:
#         dimensions = row.get('dimensions', [])
#         dateRangeValues = row.get('metrics', [])
#       for header, dimension in zip(dimensionHeaders, dimensions):
#           print header + ': ' + dimension + ';',
#         for i, values in enumerate(dateRangeValues):
#             #print '--Date range (' + str(i) + ')'
#           for metricHeader, value in zip(metricHeaders, values.get('values')):
#               print metricHeader.get('name') + ': ' + value
#               valueSum += int(value)

#   # zero is returned if response is empty
#   return str(valueSum)


service = setup_and_get_service()
views = retrieve_pageview_report(service, "/user/austin")
