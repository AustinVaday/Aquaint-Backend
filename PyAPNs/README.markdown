## PyAPNs - Apple Push Notification & AWS DynamoDB testing
The script tests reading an entry in DynamoDB database and sends iOS push notification in Python. **The actual script to run is `send_apn.py`**

### Libraries Installation
This script uses `boto3` library to read AWS DynamoDB database, and `PyAPNs` library to send push notification to Apple Push Notification Service.  

- `PyAPNs`'s git repo is cloned inside this folder, so doesn't need to be installed. **Note the program should be run in Python 2 (tested under Python 2.7)**  
- Install `boto3` by `$ python2.7 -m pip install boto3`

### Future work
The DynamoDB database is about to support multiple devices of one user; data structures should be changed here accordingly.  

### References
[Apple Push Notification](https://developer.apple.com/library/content/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/APNSOverview.html#//apple_ref/doc/uid/TP40008194-CH8-SW1)
