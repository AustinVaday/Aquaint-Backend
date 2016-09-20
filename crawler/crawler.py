import gc
import json
import pymysql
import boto3
import timeline
import sqlconf

DYNAMO_REGION = 'us-east-1'
DYNAMO_MAX_BYTES = 3500
TIMELINE_LENGTH = 30

SOURCE_TABLE = 'aquaint-newsfeed'
DEST_TABLE = 'aquaint-newsfeed-result'

def dynamo_table(table):
    return boto3.resource('dynamodb', DYNAMO_REGION).Table(table)

def dynamo_scan(db, field):
    # full db scan for field
    return []

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

def write_timeline(db, timeline):
    pass

def read_timeline(db, user):
    response = db.get_item(Key={'username': user})
    # extract response timeline data
    # construct Events from timeline data
    events = []
    return events

def json_chunk(events, to_jsonnable, max_size):
    total_len = json.dumps(map(to_jsonnable, collection))
    avg_event_len = int(total_len / len(collection))
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
    source = dynamo_table(SOURCE_TABLE)
    dest   = dynamo_table(DEST_TABLE)
    conns  = mysql_db()
    
    users = dynamo_scan(source, 'username')
    
    for user in users:
        ag = timeline.Aggregator()
        
        followees = get_followees(conns, user)
        
        for followee in followees:
            ag.load(followee, read_timeline(source, followee))
        
        timeline_result = ag.sort(TIMELINE_LENGTH)
        ag = None
        gc.collect()
        
        # batch timeline to 4kb json chunks
        timeline_jsons = json_chunk(
            timeline_result,
            lambda event: json.dumps(event.__dict__),
            DYNAMO_MAX_BYTES
        )
        
        write_timeline(dest, timeline_jsons)
