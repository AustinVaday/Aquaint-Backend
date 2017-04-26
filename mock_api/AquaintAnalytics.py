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


# Return single number to reflect total number of page views for a user
def get_user_page_views(username):
  service = setup_and_get_service()
  #view_desktop = retrieve_pageview_report(service, '/user/' + username + '/')
  view_desktop = retrieve_web_pageview_report(service, username)
  view_mobile = retrieve_pageview_report(service, '/user/' + username + '/iOS')
  return int(view_desktop) + int(view_mobile)

# Return the number of code scans a user has got
def get_user_code_scans(username):
  service = setup_and_get_service()
  #view_desktop = retrieve_pageview_report(service, '/user/' + username + '/')
  view_desktop = retrieve_web_pageview_report(service, username)
  app_scan = retrieve_pageview_report(service, '/user/' + username + '/iOS/scan')
  return int(view_desktop) + int(app_scan)

def get_user_single_page_views_for_day(username, days_ago):
  service = setup_and_get_service()
  #view_desktop = retrieve_single_pageview_report(service, username, days_ago)
  #view_mobile = retrieve_single_pageview_report(service, '/user/' + username + '/iOS', days_ago)
  #return int(view_desktop) + int(view_mobile)
  views_total = retrieve_single_pageview_report(service, username, days_ago)

# Return single number to reflect number of total engagements for all platforms combined
def get_user_total_engagements(username):
  service = setup_and_get_service()
  #click_desktop = retrieve_total_events_report(service, username)
  #click_mobile = retrieve_total_events_report(service, '/user/' + username + '/iOS')
  #return int(click_desktop) + int(click_mobile)
  clicks_total = retrieve_total_events_report(service, username)
  return clicks_total

# Return single number to reflect number of engagments for just 1 social platform
def get_user_single_engagements(username, social_platform):
  service = setup_and_get_service()
  #click_desktop_single = retrieve_single_event_report(service, username, social_platform)
  #click_mobile_single = retrieve_single_event_report(service, '/user/' + username + '/iOS', social_platform)
  #return int(click_desktop_single) + int(click_mobile_single)
  clicks_total_single = retrieve_single_event_report(service, username, social_platform)
  return clicks_total_single

# Return dictionary of social platform -> engagement count for all given social platforms
def get_user_total_engagements_breakdown(username, social_platform_list):
  service = setup_and_get_service()
  engagements_dict = dict() 
  for social_platform in social_platform_list:
    social_platform_engagements = get_user_single_engagements(username, social_platform)
    engagements_dict[social_platform] = social_platform_engagements
  return sorted_tuple_list_desc(engagements_dict) 

# Return list of tuplies of top N locations (currently just cities)
def get_user_page_views_locations(username, max_results):
  service = setup_and_get_service()
  location_dict_web = retrieve_pageview_locations_report(service, username, max_results)
  location_dict_mobile = retrieve_pageview_locations_report(service, '/user/' + username + '/iOS', max_results)
  return sorted_tuple_list_desc(union_dict(location_dict_web, location_dict_mobile))

# get report of a page view query similar to that in Query Explorer
# Use the Analytics Service Object to query the Analytics Reporting API v4

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
          'filtersExpression': ('ga:pagePath==' + webpage_url)
        }]
    }
  ).execute()
  #print "uniquePageViews-pagePath for " + webpage_url + ": " + str(response)
  # Parse the Core Reporting response dictionary and return the result integer
  #print "retrieve_pageview_report: " + webpage_url + ": " + str(parse_response_first_val(response));
  return parse_response_first_val(response)

def retrieve_web_pageview_report(service, username):
  webpage_url1 = '/user/' + username
  webpage_url2 = '/user/' + username + '/'
  response = service.reports().batchGet(
    body={
      'reportRequests' : [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate' : '365daysAgo', 'endDate' : 'today'}],
          'metrics': [{'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:pagePath'}],
          'filtersExpression': ('ga:pagePath==' + webpage_url1 + ',ga:pagePath==' + webpage_url2)
        }]
    }
  ).execute()
  
  #print str(response)
  return parse_response_all_vals_sum(response)

def retrieve_single_pageview_report(service, username, days_ago):

  startString = str(days_ago) + "daysAgo"
  endString = str(days_ago - 1) + "daysAgo"
  webpage_url1 = '/user/' + username
  webpage_url2 = '/user/' + username + '/'
  webpage_url3 = '/user/' + username + '/iOS'
  response = service.reports().batchGet(
    body={
      'reportRequests' : [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate' : startString, 'endDate' : endString}],
          'metrics': [{'expression': 'ga:uniquePageViews'}],
          'dimensions': [{'name': 'ga:pagePath'}],
          'filtersExpression': ('ga:pagePath==' + webpage_url1 + ',ga:pagePath==' + webpage_url2 + ',ga:pagePath==' + webpage_url3)
        }]
    }
  ).execute()
  #print "uniquePageViews-pagePath for " + webpage_url + ": " + str(response)
  # Parse the Core Reporting response dictionary and return the result integer
  return parse_response_all_vals_sum(response)

def retrieve_pageview_locations_report(service, username, max_results):
  webpage_url1 = '/user/' + username
  webpage_url2 = '/user/' + username + '/'
  webpage_url3 = '/user/' + username + '/iOS'
  response = service.reports().batchGet(
    body={
      'reportRequests' : [
        {
          # NOTE: In our implementation, it does not make sense to sort (since we will be storing values
          # in dictionaries, then merging dictionaries of different webpage_urls, then sort the data after
          # merge.
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate' : '365daysAgo', 'endDate' : 'today'}],
          'metrics': [{'expression': 'ga:uniquePageViews'}],
          'dimensions': [{'name': 'ga:pagePath'}, {'name': 'ga:city'}],
          #'orderBys': [{'fieldName': 'ga:uniquePageViews', 'sortOrder': 'DESCENDING'}],
          'filtersExpression': ('ga:pagePath==' + webpage_url1 + ',ga:pagePath==' + webpage_url2 + ',ga:pagePath==' + webpage_url3),
          'pageSize': max_results
        }]
    }
  ).execute()
  #print "uniquePageViews-pagePath (LOCATIONS) for " + webpage_url + ": " + str(response)
  # Parse the Core Reporting response dictionary and return the result integer
  return parse_response_all_vals(response)

def retrieve_total_events_report(service, username):
  webpage_url1 = '/user/' + username
  webpage_url2 = '/user/' + username + '/'
  webpage_url3 = '/user/' + username + '/iOS'
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
          'filtersExpression': ('ga:pagePath==' + webpage_url1 + ',ga:pagePath==' + webpage_url2 + ',ga:pagePath==' + webpage_url3)
        }]
    }
  ).execute()
  #print "totalEvents-pagePath for " + webpage_url + ": " + str(response)
  # Parse the Core Reporting response dictionary and return the result integer
  return parse_response_all_vals_sum(response)

def retrieve_single_event_report(service, username, social_platform):
  webpage_url1 = '/user/' + username
  webpage_url2 = '/user/' + username + '/'
  webpage_url3 = '/user/' + username + '/iOS'
  response = service.reports().batchGet(
    body={
      'reportRequests' : [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate' : '365daysAgo', 'endDate' : 'today'}],
          'metrics': [{'expression': 'ga:totalEvents'}],
          'dimensions': [{'name': 'ga:pagePath'},{'name': 'ga:eventAction'},{'name': 'ga:eventLabel'}],
          'filtersExpression': ('ga:pagePath==' + webpage_url1 + ',ga:pagePath==' + webpage_url2 + ',ga:pagePath==' + webpage_url3 + ';ga:eventAction==click;ga:eventLabel==' + social_platform)
        }]
    }
  ).execute()
  #print "single-pagePath for " + webpage_url + " and " + social_platform + ": " + str(response)
  # Parse the Core Reporting response dictionary and return the result integer
  return parse_response_all_vals_sum(response)


def parse_response_first_val(response):
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
      #for header, dimension in zip(dimensionHeaders, dimensions):
        #print header + ': ' + dimension
      for i, values in enumerate(dateRangeValues):
        #print '--Date range (' + str(i) + ')'
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          #print '--' + metricHeader.get('name') + ': ' + value
          return str(value)

  # zero is returned if response is empty
  #print '--Response is empty.'
  return str(0)

def parse_response_all_vals_sum(response):
  # Parses and prints the Analytics Reporting API V4 response
  # Here, we return the sum of all values in the response
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    valueSum = 0
    for row in rows:
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])
      for header, dimension in zip(dimensionHeaders, dimensions):
        print header + ': ' + dimension + ';',
        for i, values in enumerate(dateRangeValues):
          #print '--Date range (' + str(i) + ')'
          for metricHeader, value in zip(metricHeaders, values.get('values')):
            print metricHeader.get('name') + ': ' + value
            valueSum += int(value)

  # zero is returned if response is empty
  #print '--Response is empty.'
  return str(valueSum)

# Return all values in a list of tuples format
def parse_response_all_vals(response):
  # Parses and prints the Analytics Reporting API V4 response
  # Here, we return a dictionary for all values in the response
  #print response
  locations_dictionary = dict()

  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      key=""
      for header, dimension in zip(dimensionHeaders, dimensions):
        #print header + ': ' + dimension
        if header == 'ga:city' : key = dimension

      for i, values in enumerate(dateRangeValues):
        #print '--Date range (' + str(i) + ')'
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          #print '--' + metricHeader.get('name') + ': ' + value
          if key in locations_dictionary:
            locations_dictionary[key] += int(value)
          else:
            locations_dictionary[key] = int(value)

  return locations_dictionary 

# Return the union of two dictionaries
def union_dict(x, y):
  return { k: x.get(k, 0) + y.get(k, 0) for k in set(x) | set(y) }

# Sort a dictionary in descending order and convert to tuple
def sorted_tuple_list_desc(dictionary):
  # To sort by ascending, change the 1 -> 0 below
  return sorted(dictionary.items(), key=operator.itemgetter(1), reverse=True)
  

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
        #for entry in profiles.get('items'):
          #print str(entry)
        return profiles.get('items')[0].get('id')

  return None


def main():
  # How many page views does Navid get, including Aquaint-Web and Aquaint-iOS?
  username1 = 'navid'
  user_clicks = get_user_page_views(username1)
  print "User " + username1 + " has " + str(user_clicks) + " Page Views."

#  eng_dict = get_user_total_engagements_breakdown(service, username, ['instagram', 'facebook', 'snapchat'])
#  print "User engagement dictionary"
#  print eng_dict
  dicto = get_user_page_views_locations(username1, 10)
  #print "DICTIONARY:"
  #print dicto

  # Testing #2
  username2 = 'austin'
  user_web_pageviews = get_user_page_views(username2)
  user_code_scans = get_user_code_scans(username2)
  print "Testing result: get_user_page_views = " + str(user_web_pageviews) + ", get_user_code_scans = " + str(user_code_scans)

  # Systematic Test Cases, at least to make sure there are no syntax errors
  print "----Systematic Test Cases----"
  testUser = 'austin'
  print "get_user_page_views = " + str(get_user_page_views(testUser))
  print "get_user_code_scans = " + str(get_user_code_scans(testUser))
  print "get_user_single_page_views_for_day = " + str(get_user_single_page_views_for_day(testUser, 10))
  print "get_user_total_engagements = " + str(get_user_total_engagements(testUser))
  print "get_user_single_engagements = " + str(get_user_single_engagements(testUser, 'facebook'))
  print "get_user_total_engagements_breakdown = " + str(get_user_total_engagements_breakdown(testUser, ['facebook', 'instagram']))
  print "get_user_page_views_locations = " + str(get_user_page_views_locations(testUser, 15))
  print "----End Systematic Test Cases----"
  
  print "----Current Session Testing----"
  cities = get_user_page_views_locations('austin', 15)
  print cities

if __name__ == '__main__':
  main()
