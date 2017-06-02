import gc
import json

import operator
import pymysql
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import time
from datetime import datetime
import calendar
import timeline
import sqlconf

import decimal

import socket, errno

DEST_TABLE   = 'aquaint-leaderboards'

# Return unix timestamp UTC time
def get_current_timestamp():
    utc = datetime.utcnow()
    return calendar.timegm(utc.utctimetuple())

# Instantiate DynamoDB connection
def dynamo_table(table_name):
    return boto3.resource(
        'dynamodb'
    ).Table(table_name)

# Instantiate Lambda connection
def lambda_function():
    return boto3.client('lambda')

def write_data(table, metric, attributes, usernames, index, display_name):
    # Insert new records
    table.put_item(
        Item={
            'metric': metric,
            'attributes': attributes,
            'usernames': usernames,
            'lastupdated': get_current_timestamp(),
            'index': index,
            'displayname': display_name
        }
    )

# Instantiate MySQL connection
def mysql_db():
    return pymysql.connect(
        sqlconf.endpoint,
        sqlconf.username,
        "",
        sqlconf.dbname
    ).cursor()
    
# Get followers for user
def get_most_followers(cursor):
    cursor.execute(
        'SELECT followee, COUNT(*) as frequency \
        FROM follows_db.username_follows \
        GROUP BY followee \
        ORDER BY COUNT(*) DESC \
        LIMIT 15;'
    )

    # This will return usernames in one array and attributes in another
    return zip(*cursor.fetchall())
    
# Get followees for user
def get_most_followees(cursor):
    cursor.execute(
        'SELECT follower, COUNT(*) as frequency \
        FROM follows_db.username_follows \
        GROUP BY follower \
        ORDER BY COUNT(*) DESC \
        LIMIT 15;'
    )

    # This will return usernames in one array and attributes in another
    return zip(*cursor.fetchall())

# remove /user/, /, and /iOS
def parse_and_clean_string(dirty):
    print("String was: " + dirty)
    clean = dirty.replace('/user/','').replace('/iOS','').replace('/','')
    print("String is now: " + clean)
    return clean 

# Sort a dictionary in descending order and convert to tuple
def sorted_tuple_list_desc(dictionary):
  # To sort by ascending, change the 1 -> 0 below
  return sorted(dictionary.items(), key=operator.itemgetter(1), reverse=True)

def parse_and_shrink(tuple_list, des_size):
    username_to_count = dict()
    
    for tupleVal in tuple_list:
        path, count = tupleVal
        username = str(parse_and_clean_string(path))
        if not username:
            continue
        if username in username_to_count:
            username_to_count[username] += count
        else:
            username_to_count[username] = count 
    sorted_ret = sorted_tuple_list_desc(username_to_count)
    return zip(*sorted_ret[:des_size])

def aggregate():
    # Initialize databases
    dest = dynamo_table(DEST_TABLE)
    conns  = mysql_db()
    awslambda = lambda_function()

    print('Connected databases')
    
    most_followers_users, most_followers_count_list = get_most_followers(conns)
    most_followees_users, most_followees_count_list = get_most_followees(conns)

    most_views_inload = b"""{ 
        "action":"getTopProfileViews",
        "max_results":"100"
    }"""

    most_views_response = awslambda.invoke(
        FunctionName='mock_api',
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=most_views_inload,
    )

    most_engagements_inload = b"""{ 
        "action":"getTopEngagements",
        "max_results":"100"
    }"""

    most_engagements_response = awslambda.invoke(
        FunctionName='mock_api',
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=most_engagements_inload,
    )
    print("MOST VIEWS RESPONSE: ")
    print(most_views_response)
    most_views_array = most_views_response['Payload'].read()
    most_engagements_array = most_engagements_response['Payload'].read()
    #print("MOST VIEWS:")
    #print(most_views)
    
    print("MOST VIEWS SPLIT:")
    most_views_users, most_views_count_list = parse_and_shrink(json.loads(most_views_array), 15)
    most_engagements_users, most_engagements_count_list = parse_and_shrink(json.loads(most_engagements_array), 15) 

    print('mostFollowers:')
    print(list(most_followers_users))

    print('mostFollowings:')
    print(list(most_followees_users))
    
    write_data(dest, "mostFollowers", list(most_followers_count_list), list(most_followers_users), 2, "Most Followers")
    write_data(dest, "mostFollowings", list(most_followees_count_list), list(most_followees_users), 3, "Most Following")
    write_data(dest, "mostViews", list(most_views_count_list), list(most_views_users), 0, "Most Profile Views")
    write_data(dest, "mostEngagements", list(most_engagements_count_list), list(most_engagements_users), 1, "Most Profile Engagements")
    print('Done')


