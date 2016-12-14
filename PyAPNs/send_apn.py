from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import time
from apns import APNs, Frame, Payload

# Apple Push Notification: connect from provider to APN
apns = APNs(use_sandbox=True, cert_file='AquaintPN_cert.pem', key_file='AquaintPN_key.pem')

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# getting device ID from DynamoDB table
dynamodb = boto3.resource("dynamodb", region_name='us-east-1', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")

table = dynamodb.Table('aquaint-devices')

# testing only: getting the device ID of user "austin", and send a push notification to his device
username = "austin"

try:
    response = table.get_item(
        Key={
            'username': username
        }
    )
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    item = response['Item']
    print("GetItem succeeded.")

    # send an Apple Push Notification
    device_id_list = item["deviceidlist"]

    for token_hex in device_id_list:
        print(token_hex)
        payload = Payload(alert="An Apple push notification for " + username + ", data from DynamoDB and sent from Python!", sound="default", badge=1)

        apns.gateway_server.send_notification(token_hex, payload)
