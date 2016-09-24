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

TIMELINE_LENGTH = 30

def dynamo_table(table_name):
    return boto3.resource(
        'dynamodb'
    ).Table(table_name)

def dynamo_scan(table, field):
    ret = []
    last_key = None
    while True:
        opts = { 'ProjectionExpression': field }
        if last_key is not None:
            opts.update({'ExclusiveStartKey': last_key})
        
        result = table.scan(**opts)
        
        ret += map(
            lambda item: item[field],
            result['Items']
        )
        
        if 'LastEvaluatedKey' in result:
            last_key = result['LastEvaluatedKey']
        else:
            break
    
    return ret

def read_eventlist(db, user):
    response = db.get_item(Key={ 'username': user })
    
    if 'Item' not in response: return []
    
    events = response['Item']['newsfeedList']
    return map(
        lambda event: timeline.Event.from_dynamo(user, event),
        events
    )

def write_timeline(table, user, timeline_jsons):
    old = table.query(
        KeyConditionExpression=Key('username').eq(user),
        ProjectionExpression='username, seq'
    )['Items']
    
    for record in old:
        table.delete_item(Key=record)
    
    for i, timeline_json in enumerate(timeline_jsons):
        table.put_item(
            Item={
                'username': user,
                'seq': i,
                'data': timeline_json
            }
        )

def mysql_db():
    return pymysql.connect(
        sqlconf.endpoint,
        sqlconf.username,
        "",
        sqlconf.dbname
    ).cursor()
    
def get_followees(cursor, user):
    cursor.execute(
        'SELECT followee FROM username_follows WHERE follower = %s;',
        (user)
    )
    return map(
        lambda row: row[0],
        cursor.fetchall()
    )

def json_chunk(events, to_jsonnable, max_size):
    if len(events) == 0: return ['[]']

    total_len = len(
        json.dumps(
            map(to_jsonnable, events)
        )
    )
    avg_event_len = int(total_len / len(events))
    events_per_record = int(DYNAMO_MAX_BYTES / avg_event_len) - 1
    
    event_partitions = [events[i:i+events_per_record]
        for i in range(
            0,
            len(events),
            events_per_record
        )
    ]
    
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
    source = dynamo_table(SOURCE_TABLE)
    dest   = dynamo_table(DEST_TABLE)
    conns  = mysql_db()
    print('Connected databases')
    
    users = dynamo_scan(source, 'username')
    print('Found %s users' % len(users))
    
    for user in users:
        print('Processing %s' % user)
        ag = timeline.Aggregator()
        
        for followee in get_followees(conns, user):
            followee_events = read_eventlist(source, followee)
            ag.load(
                filter( # Filter out events where the current user is the only subject
                    lambda event: not (user in event.other and len(event.other) == 1),
                    followee_events
                )
            )
        
        timeline_result = ag.sort(TIMELINE_LENGTH)
        ag = None
        gc.collect()
        
        timeline_jsons = json_chunk(
            timeline_result,
            lambda event: event.__dict__,
            DYNAMO_MAX_BYTES
        )
        
        write_timeline(dest, user, timeline_jsons)

    print('Done')
