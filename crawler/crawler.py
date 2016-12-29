import gc
import json

import pymysql
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
import calendar
import timeline
import sqlconf

DYNAMO_MAX_BYTES = 3500
SOURCE_TABLE = 'aquaint-user-eventlist'
DEST_TABLE   = 'aquaint-newsfeed'
NOTIFICATION_PERIOD_SEC = 43200 #12hrs

TIMELINE_LENGTH = 60

# Return unix timestamp UTC time
def get_current_timestamp():
    utc = datetime.utcnow()
    return calendar.timegm(utc.utctimetuple())

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

# Fetch notificaiton-timestamp for user
def read_eventlist_notif(db, user):
    # Get raw data
    response = db.get_item(Key={ 'username': user })
    
    # No Events handler
    if 'Item' not in response: return 0 
    
	# If no notificationTimestamp, we create one. 
    if 'notificationTimestamp' not in response['Item']:
        write_eventlist_notif(db, user, 0)	
        return 0

    # Convert raw data to integer val 
    return response['Item']['notificationTimestamp']

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

# Write new user notification timestamp to eventlist db 
def write_eventlist_notif(table, user, notification_timestamp):
    # Update notification timestamp. Assumes notificationTimestamp exists
    table.update_item(
        Key={
            'username': user
        },
        UpdateExpression = 'SET notificationTimestamp = :val',
        ExpressionAttributeValues = {
            ':val': notification_timestamp
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
def get_recent_follows(cursor, user, start_timestamp):
    cursor.execute(
        'SELECT follower FROM username_follows WHERE followee = %s AND UNIX_TIMESTAMP(timestamp) > %d ORDER BY timestamp DESC;',
        (user),
        (start_timestamp)
    )
    return cursor.fetchall()

# Convert events to json with to_jsonnable function and paginate
def json_chunk(events, to_jsonnable, max_size):
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
    conns  = mysql_db()
    print('Connected databases')
    
    # Fetch user list
    users = dynamo_scan(source, 'username')
    print('Found %s users' % len(users))
    
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
            DYNAMO_MAX_BYTES
        )
        
        # Write constructed newsfeed to database
        write_timeline(dest, user, timeline_jsons)


        # Temp val for testing
        last_read_timestamp = 1481956586

        # Generate list of new followers for push notifications
        new_followers = get_recent_follows(conns, user, last_read_timestamp)
        print("new_followers are: %s" % new_followers)

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
#        # If we send push notification successfully, update db user with new notification timestamp
#        if did_send_notif:
#            write_eventlist_notif(source, user, get_current_timestamp())

    print('Done')
