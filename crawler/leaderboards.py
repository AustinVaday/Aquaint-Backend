import gc
import json

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

DYNAMO_MAX_BYTES = 3500
DEST_TABLE   = 'aquaint-leaderboards'

TIMELINE_LENGTH = 60
MAX_NUM_EVENTS = 15

# Return unix timestamp UTC time
def get_current_timestamp():
    utc = datetime.utcnow()
    return calendar.timegm(utc.utctimetuple())

# Instantiate DynamoDB connection
def dynamo_table(table_name):
    return boto3.resource(
        'dynamodb'
    ).Table(table_name)

def write_data(table, metric, attributes, usernames):
    # Insert new records
    table.put_item(
        Item={
            'metric': metric,
            'attributes': attributes,
            'usernames': usernames,
            'lastupdated': get_current_timestamp() 
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


def aggregate():
    # Initialize databases
    dest = dynamo_table(DEST_TABLE)
    conns  = mysql_db()
    print('Connected databases')

    
    most_followers_users, most_followers_count_list = get_most_followers(conns)
    most_followees_users, most_followees_count_list = get_most_followees(conns)

    print('mostFollowers:')
    print(list(most_followers_users))

    print('mostFollowings:')
    print(list(most_followees_users))
    
    write_data(dest, "mostFollowers", list(most_followers_count_list), list(most_followers_users))
    write_data(dest, "mostFollowings", list(most_followees_count_list), list(most_followees_users))
    print('Done')


