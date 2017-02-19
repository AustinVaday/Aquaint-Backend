import requests
import boto3
from io import BytesIO

# Set up AWS bucket
s3 = boto3.resource(
    's3'
#    aws_access_key_id= "", 
#    aws_secret_access_key = ""
)
#s3Bucket = s3Connect.get_bucket("aquaint-userfiles-mobilehub-146546989", validate=False)

user = "austin"

request_headers = {
    "X-Mashape-Key": "3AQc18gTaJmshmHWJWfKnzKtNhDEp1HcAVwjsnhOAxrcaYjCn8"
}

# Generate this string using https://market.mashape.com/unitag/qr-code-generation
request_http = "https://unitag-qr-code-generation.p.mashape.com/api?data=%7B%22TYPE%22%3A%22url%22%2C%22DATA%22%3A%7B%22URL%22%3A%22www.aquaint.us/user/" + user + "%22%7D%7D&setting=%7B%22LAYOUT%22%3A%7B%22COLORBG%22%3A%22ffffff%22%2C%22GRADIENT_TYPE%22%3A%22NO_GR%22%2C%22COLOR1%22%3A%223f729b%22%7D%2C%22EYES%22%3A%7B%22EYE_TYPE%22%3A%22ER_IR%22%7D%2C%22LOGO%22%3A%7B%22L_NAME%22%3A%22http%3A%2F%2Faquaint.us%2Fimages%2FAquaint-Social-Emblem.png%22%2C%22EXCAVATE%22%3Atrue%7D%2C%22E%22%3A%22M%22%2C%22BODY_TYPE%22%3A5%7D"

response = requests.get(request_http, headers=request_headers) 

if response.status_code == 200 or response.status_code == 201:
    # Upload file to S3
    bytesIO = BytesIO(response.content)
    s3.meta.client.upload_fileobj(bytesIO, "aquaint-userfiles-mobilehub-146546989", "public/scancodes/" + user) 
