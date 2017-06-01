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
from apns import APNs, Frame, Payload
from time import sleep

import socket, errno

# Initializing Apple Push Notification: connect from provider to APN. A key file without passphase is used here for unattended script execution.
#apns = APNs(use_sandbox=True, cert_file='/home/ubuntu/.Aquaint-PN-keys/AquaintPN_cert.pem', key_file='/home/ubuntu/.Aquaint-PN-keys/AquaintPN_key_noenc.pem')
apns = APNs(use_sandbox=False,
            cert_file='/home/ubuntu/.Aquaint-PN-Distribution/AquaintPN_Distribution_cert.pem',
            key_file='/home/ubuntu/.Aquaint-PN-Distribution/AquaintPN_Distribution_key_noenc.pem')

DYNAMO_MAX_BYTES = 3500
SOURCE_TABLE = 'aquaint-user-eventlist'
DEST_TABLE   = 'aquaint-newsfeed'
DEVICE_TABLE = 'aquaint-devices'
NOTIFICATION_PERIOD_SEC = 600 # 10 minutes
NOTIFICATION_TIMESTAMP_FILE = "notificationsLastSentTimestamp.txt"

TIMELINE_LENGTH = 60
MAX_NUM_EVENTS = 15

# Return unix timestamp UTC time
def get_current_timestamp():
    utc = datetime.utcnow()
    return calendar.timegm(utc.utctimetuple())

# Get the last time we sent notifications to users 
def get_notifications_last_sent_timestamp():
    with open(NOTIFICATION_TIMESTAMP_FILE, 'r') as file:
        return int(file.readline().rstrip())

# Set the last time we sent notifications to users
def set_notifications_last_sent_timestamp(new_timestamp):
    with open(NOTIFICATION_TIMESTAMP_FILE, 'w') as file:
        print("Wrote: %s" % new_timestamp)
        file.write(str(new_timestamp))

# Instantiate DynamoDB connection
def dynamo_table(table_name):
    return boto3.resource(
        'dynamodb'
    ).Table(table_name)

# Scan DynamoDB table for field and handle oversized reads
def dynamo_scan(table, field):
    ret = []
    last_key = None
    
    while True:
        opts = { 'ProjectionExpression': field }
        if last_key is not None:
            opts.update({'ExclusiveStartKey': last_key})
        
        result = table.scan(**opts)
        
        # Join arrays from multiple reads
        ret += map(
            lambda item: item[field],
            result['Items']
        )
        
        # Handle oversized read
        if 'LastEvaluatedKey' in result:
            last_key = result['LastEvaluatedKey']
        else:
            break
    
    return ret

# Fetch event list for user
def read_eventlist(db, user):
    # Get raw data
    response = db.get_item(Key={ 'username': user })
    
    # No Events handler
    if 'Item' not in response: return []
    
    # Convert raw data to Event objects
    events = response['Item']['newsfeedList']
    return map(
        lambda event: timeline.Event.from_dynamo(user, event),
        events
    )

## Fetch notificaiton-timestamp for user
#def read_eventlist_notif(db, user):
#    # Get raw data
#    response = db.get_item(Key={ 'username': user })
#    
#    # No Events handler
#    if 'Item' not in response: return 0 
#    
#	# If no notificationTimestamp, we create one. 
#    if 'notificationTimestamp' not in response['Item']:
#        write_eventlist_notif(db, user, 0)	
#        return 0
#
#    # Convert raw data to integer val 
#    return response['Item']['notificationTimestamp']

# Fetch list of user deviceID to send out push notifications
def read_user_device_list(db, user):
    # Get raw data
    response = db.get_item(Key={ 'username': user })
    
    # No Events handler
    if 'Item' not in response: return [] 
    
	# If no deviceId 
    if 'deviceidlist' not in response['Item']: return []

    # Convert raw data to list datastructure 
    return response['Item']['deviceidlist']

# Write new newsfeed to database
def write_timeline(table, user, timeline_jsons):
    # Fetch old records
    old = table.query(
        KeyConditionExpression=Key('username').eq(user),
        ProjectionExpression='username, seq'
    )['Items']
    
    # Delete old records
    for record in old:
        table.delete_item(Key=record)
    
    # Insert new records
    for i, timeline_json in enumerate(timeline_jsons):
        table.put_item(
            Item={
                'username': user,
                'seq': i,
                'data': timeline_json
            }
        )

## Write new user notification timestamp to eventlist db 
#def write_eventlist_notif(table, user, notification_timestamp):
#    # Update notification timestamp. Assumes notificationTimestamp exists
#    table.update_item(
#        Key={
#            'username': user
#        },
#        UpdateExpression = 'SET notificationTimestamp = :val',
#        ExpressionAttributeValues = {
#            ':val': notification_timestamp
#        }
#    )

# Instantiate MySQL connection
def mysql_db():
    return pymysql.connect(
        sqlconf.endpoint,
        sqlconf.username,
        "",
        sqlconf.dbname
    ).cursor()
    
# Get followees for user
def get_followees(cursor, user):
    cursor.execute(
        'SELECT followee FROM username_follows WHERE follower = %s;',
        (user)
    )
    return map(
        lambda row: row[0],
        cursor.fetchall()
    )

# Get all followers of user after a particular point in time
# Note that we do not include followers that were user-approved. This will be separate
def get_recent_public_follows(cursor, user, start_timestamp):
    cursor.execute(
        'SELECT follower FROM username_follows WHERE followee = %s AND ' + \
        'UNIX_TIMESTAMP(timestamp) > %s AND userapproved = 0 ORDER BY timestamp DESC;',
        (user,
         start_timestamp)
    )
    return [i[0] for i in cursor.fetchall()]
    
# Get all users that have sent this user a follow request after a particular point in time
# (Still have to consider the case where user goes from private -> public in the future)
def get_recent_follow_requests(cursor, user, start_timestamp):
    cursor.execute(
        'SELECT follower FROM username_follow_requests WHERE followee = %s AND ' + \
        'UNIX_TIMESTAMP(timestamp) > %s ORDER BY timestamp DESC;',
        (user,
         start_timestamp)
    )
    return [i[0] for i in cursor.fetchall()]

# Get all users that have recently accepted this user's follow reuqest after a particular point in time 
def get_recent_follow_accepts(cursor, user, start_timestamp):
    cursor.execute(
        'SELECT followee FROM username_follows WHERE follower = %s AND ' + \
        'UNIX_TIMESTAMP(timestamp) > %s AND userapproved = 1 ORDER BY timestamp DESC;',
        (user,
         start_timestamp)
    )
    return [i[0] for i in cursor.fetchall()]

# Convert events to json with to_jsonnable function and paginate
def json_chunk(events, to_jsonnable, max_size, max_num_events):
    # No events handler
    if len(events) == 0: return ['[]']

    # Estimate page JSON size
    total_len = len(
        json.dumps(
            map(to_jsonnable, events)
        )
    )
    avg_event_len = int(total_len / len(events))
    events_per_record = int(max_size / avg_event_len) - 1
    events_per_record = min(events_per_record, max_num_events)    

    # Paginate events based on JSON size estimate
    event_partitions = [
        events[i:i+events_per_record] for i in range(
            0,
            len(events),
            events_per_record
        )
    ]
    
    # Convert pages to json
    return map(
        lambda events: json.dumps(
            map(
                to_jsonnable,
                events
            )
        ),
        event_partitions
    )

def crawl():
    # Initialize databases
    source = dynamo_table(SOURCE_TABLE)
    dest   = dynamo_table(DEST_TABLE)
    device_table = dynamo_table(DEVICE_TABLE)
    conns  = mysql_db()
    print('Connected databases')
    
    # Fetch user list
    users = dynamo_scan(source, 'username')
    print('Found %s users' % len(users))
    
    # Get last notification sent time, via local file on server 
    last_read_timestamp = get_notifications_last_sent_timestamp()
    current_timestamp = get_current_timestamp()
    send_push_notifications = (current_timestamp - last_read_timestamp) > NOTIFICATION_PERIOD_SEC
    # A testing flag to enforce sending push notification
    #send_push_notifications = True
    print "It's time to send push notifications? " + str(send_push_notifications)

    # Iterate over all users
    for user in users:
        print('Processing %s' % user)
        # Create new timeline aggregator
        ag = timeline.Aggregator()
        
        for followee in get_followees(conns, user):
            # Get the event list of everyone the user follows
            followee_events = read_eventlist(source, followee)
            
            # Load the event list into the aggregator
            ag.load(
                # Remove events where the current user is the only subject
                filter(
                    lambda event, user=user: not (user in event.other and len(event.other) == 1),
                    followee_events
                )
            )
        
        # Sort off only enough events to fill timeline
        timeline_result = ag.sort(TIMELINE_LENGTH)
        
        # Delete aggregator and remaining events
        ag = None
        gc.collect()
        
        # Convert timeline into JSON strings paginated for Dynamo size limit
        timeline_jsons = json_chunk(
            timeline_result,
            lambda event: event.__dict__,
            DYNAMO_MAX_BYTES,
            MAX_NUM_EVENTS
        )
        
        # Write constructed newsfeed to database
        write_timeline(dest, user, timeline_jsons)

        # Detect whether we need to send notifications now 
        if send_push_notifications:
            # Get list of user deviceIDs to send push notifications to
            user_device_list = read_user_device_list(device_table, user)
            if len(user_device_list) == 0:
                # No point in processing the rest if user doesn't have any devices
                continue

            print("User %s has devices: %s" % (user, user_device_list))

            # Generate list of new followers for push notifications
            new_public_followers = get_recent_public_follows(conns, user, last_read_timestamp)

            if len(new_public_followers) > 0:
                print("new_followers are: %s" % new_public_followers)
                # Send push notifications for new public followers, for device in user_device_list:
                for token_hex in user_device_list:
                    if len(new_public_followers) == 1:
                        pn_text = "Hey " + user + ", " + new_public_followers[0] + " is now following you! "
                    else:
                        pn_text = "Hey " + user + ", " + new_public_followers[0] + " and " + str(len(new_public_followers) - 1) + " others are now following you! "
                    payload = Payload(alert=pn_text, sound="default", badge=1, custom={'identifier':"newFollower"})
                    try:
                        apns.gateway_server.send_notification(token_hex, payload)
                        #print "---APN-token_hex---:" + token_hex + ":---pn_text---:" + pn_text
                        print "Send new_public_followers notification to " + user + " with device ID " + token_hex
                        #sleep(1)
                    except socket.error, e:
                        if e[0] == errno.EPIPE:
                            print "Broken Pipe Exception: " + user + " has invalid device ID " + token_hex
                        else:
                            print "--Caught Exception when sending Push Notification: " + str(e)

            # Generate list of new follow requests for push notifications
            new_follow_requests = get_recent_follow_requests(conns, user, last_read_timestamp)
            
            if len(new_follow_requests) > 0:
                print("new_follow_requests are: %s" % new_follow_requests)
                # Send push notifications for new follow requests, for device in user_device_list:
                for token_hex in user_device_list:
                    if len(new_follow_requests) == 1:
                        pn_text = "Hey " + user + ", you have a new follow request from " + new_follow_requests[0] + "! "
                    else:
                        pn_text = "Hey " + user + ", you have new follow requests from " + new_follow_requests[0] + " and " + str(len(new_follow_requests) - 1) + " others! "
                    payload = Payload(alert=pn_text, sound="default", badge=1, custom={'identifier':"newFollowRequests"})
                    try:
                        apns.gateway_server.send_notification(token_hex, payload)
                        print "Send new_follow_requests notification to " + user + " with device ID " + token_hex
                        #sleep(1)
                    except socket.error, e:
                        if e[0] == errno.EPIPE:
                            print "Broken Pipe Exception: " + user + " has invalid device ID " + token_hex;
                        else:
                            print "--Caught Exception when sending Push Notification: " + str(e)

            # Generate list of others that have accepted this user's follow requests
            new_follow_accepts = get_recent_follow_accepts(conns, user, last_read_timestamp)
            
            if len(new_follow_accepts) > 0:
                print("new_follow_accepts are: %s" % new_follow_accepts)
                # Send push notifications for new follow accepts, for device in user_device_list:
                for token_hex in user_device_list:
                    if len(new_follow_accepts) == 1:
                        pn_text = "Hey " + user + ", your follow request to " + new_follow_accepts[0] + " is accepted! "
                    else:
                        pn_text = "Hey " + user + ", your follow requests to " + new_follow_accepts[0] + " and " + str(len(new_follow_accepts) - 1) + " others are accepted! "
                    payload = Payload(alert=pn_text, sound="default", badge=1, custom={'identifier':"followRequestAcceptance"})
                    try:
                        apns.gateway_server.send_notification(token_hex, payload)
                        print "Send new_follow_accepts notification to " + user + " with device ID " + token_hex
                        #sleep(1)
                    except socket.error, e:
                        if e[0] == errno.EPIPE:
                            print "Broken Pipe Exception: " + user + " has invalid device ID " + token_hex;
                        else:
                            print "--Caught Exception when sending Push Notification: " + str(e)

                
#########> Below code was written before privacy settings implemented. We will attempt to use a better 
#########> Approach that will work for both
#        notification_timestamp = read_eventlist_notif(source, user) 
#        print("Notification time stamp for user %s is %d" % (user, notification_timestamp)) 
#
#        # Generate list of new followers for push notifications
#        new_followers_list = [event.other[0] for event in read_eventlist(source, user) if (event.time - notification_timestamp) > NOTIFICATION_PERIOD_SEC and event.event == 'newfollower']
#        new_followers = set(new_follers_list)
#
#        print("new_follers are: %s" % new_followers)
#
#        # PUSH NOTIFICATIONS CODE HERE
#	
#        # Make sure to make use of this variable below 
#        # Will determine whether we write a new notification timestamp or not later in the script
#        did_send_notif = False
#
#        if did_send_notif:
#        # If we send push notification successfully, update db user with new notification timestamp
#            write_eventlist_notif(source, user, get_current_timestamp())

    
    if send_push_notifications:
        set_notifications_last_sent_timestamp(current_timestamp)

    print('Done')


