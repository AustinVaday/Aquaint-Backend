import time
from apns import APNs, Frame, Payload

apns = APNs(use_sandbox=True,
            cert_file='/home/ubuntu/.Aquaint-PN-Distribution/AquaintPN_Distribution_cert.pem',
            key_file='/home/ubuntu/.Aquaint-PN-Distribution/AquaintPN_Distribution_key_noenc.pem')

#token_hex = '5e41aa37230ef45d86b23c349fc066bf91fd035bd1886ce2ef60f566656df2af'
#payload = Payload(alert="PyAPNs notification testing in Python.", sound="default", badge=1)

sending_devices = ['5e41aa37230ef45d86b23c349fc066bf91fd035bd1886ce2ef60f566656df2af']

for token_hex in sending_devices:
    pn_text1 = "PyAPNs Notification Test: newFollower"
    payload1 = Payload(alert=pn_text1, sound="default", badge=1, custom={'identifier':"newFollower"})
    apns.gateway_server.send_notification(token_hex, payload1)

    pn_text2 = "PyAPNs Notification Test: followRequestAcceptance"
    payload2 = Payload(alert=pn_text2, sound="default", badge=1, custom={'identifier':"followRequestAcceptance"})
    apns.gateway_server.send_notification(token_hex, payload2)

    pn_text3 = "PyAPNs Notification Test: newFollowRequests"
    payload3 = Payload(alert=pn_text3, sound="default", badge=1, custom={'identifier':"newFollowRequests"})
    apns.gateway_server.send_notification(token_hex, payload3)

    print "Sent test notification to: " + token_hex
    