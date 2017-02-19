import requests
import boto3
from PIL import Image
from io import BytesIO

# Set up AWS bucket
s3 = boto3.resource(
    's3',
    aws_access_key_id= "", 
    aws_secret_access_key = ""
)
#s3Bucket = s3Connect.get_bucket("aquaint-userfiles-mobilehub-146546989", validate=False)


request_headers = {
    "X-Mashape-Key": ""
}

response = requests.get("https://unitag-qr-code-generation.p.mashape.com/api?data=%7B%22TYPE%22%3A%22text%22%2C%22DATA%22%3A%7B%22TEXT%22%3A%22Hello+World!%22%7D%7D&setting=%7B%22LAYOUT%22%3A%7B%22COLORBG%22%3A%22ffffff%22%2C%22GRADIENT_TYPE%22%3A%22NO_GR%22%2C%22COLOR1%22%3A%22000000%22%7D%2C%22EYES%22%3A%7B%22EYE_TYPE%22%3A%22Simple%22%7D%2C%22E%22%3A%22M%22%2C%22BODY_TYPE%22%3A0%7D", headers=request_headers) 

if response.status_code == 200 or response.status_code == 201:
    # Upload file to S3
    image = Image.open(BytesIO(response.content))
    image.save('tempImg', 'png')
    s3.meta.client.upload_file('tempImg', "aquaint-userfiles-mobilehub-146546989", "scancodes/QR-CODE.png") 

    os.remove('./tempImg')
#    keyObj = Key(s3Bucket)
#    keyObj.key = "scancodes/QR-CODE.png"
#    keyObj.content_type = response.headers['content-type']
#    keyObj.set_contents_from_string(response.content)
