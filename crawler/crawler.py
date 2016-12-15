import gc
import json

import pymysql
import boto3
from boto3.dynamodb.conditions import Key

import timeline
import sqlconf

DYNAMO_MAX_BYTES = 3500
SOURCE_TABLE = 'aquaint-user-eventlist'
DEST_TABLE   = 'aquaint-newsfeed'

TIMELINE_LENGTH = 60

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

    print('Done')
