import gc
import json

import pymysql
import boto3

import timeline
import sqlconf

DYNAMO_REGION = 'us-east-1'
DYNAMO_MAX_BYTES = 3500
SOURCE_TABLE = 'aquaint-newsfeed'
DEST_TABLE = 'aquaint-newsfeed-result'

TIMELINE_LENGTH = 30

def dynamo_table(table_name):
    return boto3.resource(
        'dynamodb',
        endpoint_url='https://dynamodb.us-east-1.amazonaws.com'
    ).Table(table_name)

def dynamo_scan(table, field):
    ret = []
    last_key = {}
    while True:
        print('DynamoDB Scan')
        result = table.scan(
            ProjectionExpression=field,
            ExclusiveStartKey=last_key
        )
        print('Got %s items' % len(result['Items']))
        ret.append(result['Items'][field])
        last_key = result['LastEvaluatedKey']
        if len(last_key) == 0: break
    
    return ret

def read_timeline(db, user):
    response = db.get_item(Key={'username': user})
    return map(timeline.Event.from_dynamo, response['Item']['newsfeedList'])

def write_timeline(table, user, timeline_jsons):
    old = table.query(
        KeyConditions={ 'username': user },
        ProjectionExpression='username, sequence'
    )['Items']
    
    for record in old:
        table.delete_item(Key=record)
    
    for i, timeline_json in enumerate(timeline_jsons):
        table.put_item(
            Item={
                'username': user,
                'sequence': i,
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
    return map(lambda row: row[0], cursor.fetchall())

def json_chunk(events, to_jsonnable, max_size):
    total_len = json.dumps(map(to_jsonnable, events))
    avg_event_len = int(total_len / len(events))
    events_per_record = int(DYNAMO_MAX_BYTES / avg_event_len) - 1
    
    event_partitions = [events[i:i+4] for i in range(0, len(events), events_per_record)]
    
    return map(
        lambda events: json.dumps(
            map(
                to_jsonnable,
                events
            )
        ),
        event_partitions
    )

def handler(event, context):
    print('Connecting DynamoDB')
    source = dynamo_table(SOURCE_TABLE)
    dest   = dynamo_table(DEST_TABLE)
    
    print('Connecting MySQL')
    conns  = mysql_db()
    
    print('Getting DynamoDB user listing')
    users = dynamo_scan(source, 'username')
    
    print('Enumerating users')
    for user in users:
        print('Processing %s.' % user)
        ag = timeline.Aggregator()
        
        followees = get_followees(conns, user)
        
        for followee in followees:
            ag.load(followee, read_timeline(source, followee))
        
        timeline_result = ag.sort(TIMELINE_LENGTH)
        ag = None
        gc.collect()
        
        timeline_jsons = json_chunk(
            timeline_result,
            lambda event: event.__dict__,
            DYNAMO_MAX_BYTES
        )
        
        write_timeline(dest, user, timeline_jsons)
