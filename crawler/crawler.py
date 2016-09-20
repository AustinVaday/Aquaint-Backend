import gc
import sqlalchemy
import boto3
import timeline

DYNAMO_REGION = 'us-east-1'
TIMELINE_LENGTH = 30

SOURCE_TABLE = 'aquaint-newsfeed'
DEST_TABLE = 'aquaint-newsfeed-result'

def dynamo_table(table):
    return boto3.resource('dynamodb', DYNAMO_REGION).Table(table)

def dynamo_scan(db, field):
    # full db scan for field
    pass

def write_timeline(db, timeline):
    pass

def read_timeline(db):
    response = source_db.get_item(Key={'username': user})
    # extract response timeline data
    # construct Events from timeline data
    # return events

def json_chunk(collection, to_json, max_size):
    # json serialize block to get length
    # divide length by event count
    # serialize chunks by dividend, checking each length
    pass

def handler(event, context):
    source = dynamo_table(SOURCE_TABLE)
    dest   = dynamo_table(DEST_TABLE)
    
    # get user listing
    users = dynamo_scan(source, 'username')
    
    for user in users:
        ag = timeline.Aggregator()
        
        # get followee listing
        followees = []
        
        for followee in followees:
            ag.load(read_timeline(source, user))
        
        timeline_result = ag.sort(TIMELINE_LENGTH)
        ag = None
        gc.collect()
        
        # batch timeline to 4kb json chunks
        timeline_jsons = json_chunk(
            timeline_result,
            lambda event: json.dumps(event.__dict__),
            DYNAMO_MAX_SIZE
        )
        
        write_timeline(dest, timeline_jsons)
