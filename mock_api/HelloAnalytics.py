"""A simple example of how to access the Google Analytics API."""

import argparse

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

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


def get_user_page_views(service, username):
  click_desktop = retrieve_pageview_report(service, '/user/' + username + '/')
  click_mobile = retrieve_pageview_report(service, '/user/' + username + '/iOS')
  return str(int(click_desktop) + int(click_mobile))

# get report of a page view query similar to that in Query Explorer
# Use the Analytics Service Object to query the Analytics Reporting API v4
def retrieve_pageview_report(service, webpage_url):
  response = service.reports().batchGet(
    body={
      'reportRequests' : [
        {
          # On the format of fields used in Query Explorer, see:
          # https://developers.google.com/analytics/devguides/reporting/core/v4/samples
          # https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate' : '365daysAgo', 'endDate' : 'today'}],
          'metrics': [{'expression': 'ga:totalEvents'}],
          'dimensions': [{'name': 'ga:pagePath'}],
          'filtersExpression': ('ga:pagePath==' + webpage_url)
        }]
    }
  ).execute()
  print "totalEvents-pagePath for " + webpage_url + ": " + str(response)
  # Parse the Core Reporting response dictionary and return the result integer
  return parse_pageview_response(response)


def parse_pageview_response(response):
  # Parses and prints the Analytics Reporting API V4 response
  # Here, we return the first value in the response, as only one value is expected
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        print header + ': ' + dimension

      for i, values in enumerate(dateRangeValues):
        print '--Date range (' + str(i) + ')'
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print '--' + metricHeader.get('name') + ': ' + value
          return value

  # zero is returned if response is empty
  print '--Response is empty.'
  return 0
  
def get_first_profile_id(service):
  # Use the Analytics service object to get the first profile id.

  # Get a list of all Google Analytics accounts for this user
  accounts = service.management().accounts().list().execute()

  if accounts.get('items'):
    # Get the first Google Analytics account.
    account = accounts.get('items')[0].get('id')

    # Get a list of all the properties for the first account.
    properties = service.management().webproperties().list(
        accountId=account).execute()

    if properties.get('items'):
      # Get the first property id.
      property = properties.get('items')[0].get('id')

      # Get a list of all views (profiles) for the first property.
      profiles = service.management().profiles().list(
          accountId=account,
          webPropertyId=property).execute()

      if profiles.get('items'):
        # return the first view (profile) id.
        # DEBUG: show all we have for profiles
        for entry in profiles.get('items'):
          print str(entry)
        return profiles.get('items')[0].get('id')

  return None


def get_results(service, profile_id):
  # Use the Analytics Service Object to query the Core Reporting API
  # for the number of sessions within the past seven days.
  return service.data().ga().get(
      ids='ga:' + profile_id,
      start_date='14daysAgo',
      end_date='today',
      metrics='ga:sessions').execute()


def print_results(results):
  # Print data nicely for the user.
  if results:
    print 'View (Profile): %s' % results.get('profileInfo').get('profileName')
    print 'Total Sessions: %s' % results.get('rows')[0][0]

  else:
    print 'No results found'


def main():
  # Define the auth scopes to request.
  scope = ['https://www.googleapis.com/auth/analytics.readonly']

  # Use the developer console and replace the values with your
  # service account email and relative location of your key file.
  service_account_email = 'aquaint-service-account@aquaint-analytics.iam.gserviceaccount.com'
  key_file_location = 'Aquaint-Analytics-acba58fc01fc.json'

  # Authenticate and construct service.
  service = get_service('analytics', 'v4', scope, key_file_location,
    service_account_email)
  #response = get_report(service)

  # How many page views does Navid get, including Aquaint-Web and Aquaint-iOS?
  username = 'navid'
  user_clicks = get_user_page_views(service, username)
  print "User " + username + " has " + user_clicks + " Impressions."
  #profile = get_first_profile_id(service)
  #print_results(get_results(service, profile))


if __name__ == '__main__':
  main()
