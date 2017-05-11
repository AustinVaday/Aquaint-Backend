import mock_api
receipt = open("iapReceiptSample.json", 'r')
r_json = receipt.read()
mock_api.handler({"action": "subscriptionGetExpiresDate", "target": "austin", "receipt_json": r_json}, "")