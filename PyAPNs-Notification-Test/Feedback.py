import sys, time
from apns import APNs, Frame, Payload

# Note that it works fine to use sandbox mode with distribution keys
feedback_connection = APNs(use_sandbox=False,
            cert_file='/home/ubuntu/.Aquaint-PN-Distribution/AquaintPN_Distribution_cert.pem',
            key_file='/home/ubuntu/.Aquaint-PN-Distribution/AquaintPN_Distribution_key_noenc.pem')


for (token_hex, fail_time) in feedback_connection.feedback_server.items():
    print "token_hex = " + token_hex + "; fail_time: " + fail_time
