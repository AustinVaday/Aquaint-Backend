import pymysql, sqlconf, boto3, requests 
import AquaintAnalytics
import stripe, stripeconf, json
from io import BytesIO
import itunesiap, itunesiapconf #In-app payments


def sql_select(sql, query):
    cursor = sql.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return results

def sql_cd(sql, query):
    cursor = sql.cursor()
    count = cursor.execute(query)
    sql.commit()
    return count

def adduser(event, sql):
    if 'realname' not in event: raise RuntimeError("Please specify 'realname'.")
    
    query = "INSERT INTO users (username, realname) VALUES('{target}', '{realname}');".format(
        target = event['target'],
        realname = event['realname']
    )
    
    return sql_cd(sql, query)

def updatern(event, sql):
    if 'realname' not in event: raise RuntimeError("Please specify 'realname'.")
    
    query = "UPDATE users SET realname = '{realname}' WHERE username = '{target}';".format(
        realname = event['realname'],
        target = event['target']
    )
    
    return sql_cd(sql, query)

def simplesearch(event, sql):
    if 'start' not in event or 'end' not in event:
        raise RuntimeError("Please specify range 'start' and 'end'.")
    
    sqlstart = max(event['start'], 0)
    sqlcount = max(sqlstart, event['end']) - sqlstart
    
    query = ("SELECT username FROM users WHERE username LIKE '%{target}%' " + \
        "OR realname LIKE '%{target}%' LIMIT {start}, {count};")\
        .format(
            target = event['target'],
            start = sqlstart,
            count = sqlcount
        )
    
    
    return list(
        map(
            lambda x: x[0],
            sql_select(sql, query)
        )
    )


def follow(event, sql):
    if 'me' not in event: raise RuntimeError("Please specify 'me'.")
    if 'userapproved' not in event: 
        user_approved = 0 
    else: 
        user_approved = event['userapproved']

    query = ("INSERT INTO follows (follower, followee, userapproved) VALUES (" + \
            "(SELECT user_index FROM users WHERE username = '{me}'), " + \
            "(SELECT user_index FROM users WHERE username = '{target}'), " + \
            "({userapproved}));").format(
            me = event['me'],
            target = event['target'],
            userapproved = user_approved
        )
    
    return sql_cd(sql, query)

def unfollow(event, sql):
    if 'me' not in event: raise RuntimeError("Please specify 'me'.")
    query = ("DELETE FROM follows " + \
        "WHERE follower = (SELECT user_index FROM users WHERE username = '{me}') " + \
        "AND followee = (SELECT user_index FROM users WHERE username = '{target}');").format(
            me = event['me'],
            target = event['target']
        )

    return sql_cd(sql, query)


def followRequest(event, sql):
    if 'me' not in event: raise RuntimeError("Please specify 'me'.")
    query = ("INSERT INTO follow_requests (follower, followee) VALUES (" + \
            "(SELECT user_index FROM users WHERE username = '{me}'), " + \
            "(SELECT user_index FROM users WHERE username = '{target}') " + \
        ");").format(
            me = event['me'],
            target = event['target']
        )
    
    return sql_cd(sql, query)

def unfollowRequest(event, sql):
    if 'me' not in event: raise RuntimeError("Please specify 'me'.")
    query = ("DELETE FROM follow_requests " + \
        "WHERE follower = (SELECT user_index FROM users WHERE username = '{me}') " + \
        "AND followee = (SELECT user_index FROM users WHERE username = '{target}');").format(
            me = event['me'],
            target = event['target']
        )

    return sql_cd(sql, query)

def getNumFollowers(event, sql):
    query = "SELECT COUNT(follower) FROM username_follows WHERE followee = '{}';".format(
        event['target']
    )
    
    return sql_select(sql, query)[0][0]


def getNumFollowees(event, sql):
    query = "SELECT COUNT(followee) FROM username_follows WHERE follower = '{}';".format(
        event['target']
    )
    
    return sql_select(sql, query)[0][0]

def getNumFollowRequests(event, sql):
    query = "SELECT COUNT(follower) FROM username_follow_requests WHERE followee = '{}';".format(
        event['target']
    )
    
    return sql_select(sql, query)[0][0]
  
def getFollowers(event, sql):
    if 'start' not in event or 'end' not in event:
        query = ("SELECT follower, UNIX_TIMESTAMP(timestamp) FROM username_follows " + \
            "WHERE followee = '{}' ORDER BY timestamp DESC;").format(
            event['target']
            )

        return sql_select(sql, query)
    else:
        sqlstart = max(event['start'], 0)
        sqlcount = max(sqlstart, event['end']) - sqlstart

        query = ("SELECT follower, UNIX_TIMESTAMP(timestamp) FROM username_follows " + \
            "WHERE followee = '{target}' ORDER BY timestamp DESC LIMIT {start}, {count};").format(
                target = event['target'],
                start = sqlstart,
                count = sqlcount
                )
        return sql_select(sql, query)

def getFollowees(event, sql):
    if 'start' not in event or 'end' not in event:
        query = ("SELECT followee, UNIX_TIMESTAMP(timestamp) FROM username_follows " + \
            "WHERE follower = '{}' ORDER BY timestamp DESC;").format(
            event['target']
            )

        return sql_select(sql, query)
    else:
        sqlstart = max(event['start'], 0)
        sqlcount = max(sqlstart, event['end']) - sqlstart

        query = ("SELECT followee, UNIX_TIMESTAMP(timestamp) FROM username_follows " + \
            "WHERE follower = '{target}' ORDER BY timestamp DESC LIMIT {start}, {count};").format(
                target = event['target'],
                start = sqlstart,
                count = sqlcount
                )
        return sql_select(sql, query)

def getFollowersDict(event, sql):
    return dict(getFollowers(event,sql))

def getFolloweesDict(event, sql):
    return dict(getFollowees(event,sql))

def getFollowerRequests(event, sql):
    if 'start' not in event or 'end' not in event:
        query = ("SELECT follower, UNIX_TIMESTAMP(timestamp) FROM username_follow_requests " + \
            "WHERE followee = '{}' ORDER BY timestamp DESC;").format(
            event['target']
            )

        return sql_select(sql, query)
    else:
        sqlstart = max(event['start'], 0)
        sqlcount = max(sqlstart, event['end']) - sqlstart

        query = ("SELECT follower, UNIX_TIMESTAMP(timestamp) FROM username_follow_requests " + \
            "WHERE followee = '{target}' ORDER BY timestamp DESC LIMIT {start}, {count};").format(
                target = event['target'],
                start = sqlstart,
                count = sqlcount
                )
        return sql_select(sql, query)

def getFolloweeRequests(event, sql):
    if 'start' not in event or 'end' not in event:
        query = ("SELECT followee, UNIX_TIMESTAMP(timestamp) FROM username_follow_requests " + \
            "WHERE follower = '{}' ORDER BY timestamp DESC;").format(
            event['target']
            )

        return sql_select(sql, query)
    else:
        sqlstart = max(event['start'], 0)
        sqlcount = max(sqlstart, event['end']) - sqlstart

        query = ("SELECT followee, UNIX_TIMESTAMP(timestamp) FROM username_follow_requests " + \
            "WHERE follower = '{target}' ORDER BY timestamp DESC LIMIT {start}, {count};").format(
                target = event['target'],
                start = sqlstart,
                count = sqlcount
                )
        return sql_select(sql, query)

def getFollowerRequestsDict(event, sql):
    return dict(getFollowerRequests(event,sql))

def getFolloweeRequestsDict(event, sql):
    return dict(getFolloweeRequests(event,sql))

def doIFollow(event, sql):
    if 'me' not in event: raise RuntimeError("Please specify 'me'.")
    query = ("SELECT COUNT(*) FROM follows " + \
        "WHERE follower = (SELECT user_index FROM users WHERE username = '{me}') " + \
        "AND followee = (SELECT user_index FROM users WHERE username = '{target}');").format(
            me = event['me'],
            target = event['target']
        )
    
    return sql_select(sql, query)[0][0]

def didISendFollowRequest(event, sql):
    if 'me' not in event: raise RuntimeError("Please specify 'me'.")
    query = ("SELECT COUNT(*) FROM follow_requests " + \
        "WHERE follower = (SELECT user_index FROM users WHERE username = '{me}') " + \
        "AND followee = (SELECT user_index FROM users WHERE username = '{target}');").format(
            me = event['me'],
            target = event['target']
        )
    
    return sql_select(sql, query)[0][0]

def createScanCodeForUser(event): 
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")

    # Set up AWS bucket
    s3 = boto3.resource('s3')

    request_headers = {
        "X-Mashape-Key": "3AQc18gTaJmshmHWJWfKnzKtNhDEp1HcAVwjsnhOAxrcaYjCn8"
    }

    # Generate this string using https://market.mashape.com/unitag/qr-code-generation
    request_http = "https://unitag-qr-code-generation.p.mashape.com/api?data=%7B%22TYPE%22%3A%22url%22%2C%22DATA%22%3A%7B%22URL%22%3A%22www.aquaint.us/user/" + event["target"] + "%22%7D%7D&setting=%7B%22LAYOUT%22%3A%7B%22COLORBG%22%3A%22transparent%22%2C%22GRADIENT_TYPE%22%3A%22NO_GR%22%2C%22COLOR1%22%3A%223f729b%22%7D%2C%22EYES%22%3A%7B%22EYE_TYPE%22%3A%22ER_IR%22%7D%2C%22LOGO%22%3A%7B%22L_NAME%22%3A%22http%3A%2F%2Faquaint.us%2Fstatic%2Fimages%2FAquaint-Social-Emblem-Transparent.png%22%2C%22EXCAVATE%22%3Atrue%7D%2C%22E%22%3A%22M%22%2C%22BODY_TYPE%22%3A5%7D"

    response = requests.get(request_http, headers=request_headers) 

    if response.status_code == 200 or response.status_code == 201:
        # Upload file to S3
        bytesIO = BytesIO(response.content)
        s3.meta.client.upload_fileobj(bytesIO, "aquaint-userfiles-mobilehub-146546989", "public/scancodes/" + event["target"]) 
        return 1
    else:
        return -1

def getUserPageViews(event):
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")
    return AquaintAnalytics.get_user_page_views(event["target"])

def getUserCodeScans(event):
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")
    return AquaintAnalytics.get_user_code_scans(event["target"])
    
def getUserSinglePayViewsForDay(event):
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")
    if 'days_ago' not in event: raise RuntimeError("Please specify 'days_ago'.")
    return AquaintAnalytics.get_user_single_page_views_for_day(event["target"], event["days_ago"])

def getUserTotalEngagements(event):
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")
    return AquaintAnalytics.get_user_total_engagements(event["target"])

def getUserSingleEngagements(event):
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")
    if 'social_platform' not in event: raise RuntimeError("Please specify 'social_platform'.")
    return AquaintAnalytics.get_user_single_engagements(event["target"], event["social_platform"])

def getUserTotalEngagementsBreakdown(event):
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")
    if 'social_list' not in event: raise RuntimeError("Please specify 'social_list'.")
    return AquaintAnalytics.get_user_total_engagements_breakdown(event["target"], event["social_list"])

def getUserPageViewsLocations(event):
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")
    if 'max_results' not in event: raise RuntimeError("Please specify 'max_results'.")
    return AquaintAnalytics.get_user_page_views_locations(event["target"], event["max_results"])

# # Doc ref : https://stripe.com/docs/mobile/ios/standard
# # This function should only be called ONCE. 
# def createPaymentCustomer(event, sql):
#     if 'target' not in event: raise RuntimeError("Please specify 'target'.")
#     if 'email' not in event:raise RuntimeError("Please specify 'email'") 
#     stripe.api_key = stripeconf.api_key
#     response = stripe.Customer.create(
#         description = "Aquaint Aqualytics Customer",
#         email = event["email"]
#     )  

#     # NOTE: This will only update users that do not have customer IDs. Customer IDs should and must not be changed
#     # when emails are changed. 
#     query = "UPDATE users SET customerid = '{cust_id}' WHERE username = '{target}' AND customerid IS NULL;".format(
#         cust_id = response["id"],
#         target = event['target']
#     )    

#     sql_cd(sql, query)
#     return getCustomerIdFromUserName(event["target"], sql)

# # This method is called to populate the user's list of payment methods in our UI.
# # Note, we need to cast customer to string due to weird JSON issue:
# #   https://github.com/stripe/stripe-python/issues/220
# def getPaymentCustomerObject(event, sql):
#     if 'target' not in event: raise RuntimeError("Please specify 'target'.")
#     cust_id = getCustomerIdFromUserName(event["target"], sql)
#     print "cust id " + cust_id
#     stripe.api_key = stripeconf.api_key
#     print "api_key " + stripe.api_key 
#     customer = stripe.Customer.retrieve(cust_id)
#     return str(customer)

# # This method is called when the user adds a new payment method via our UI
# def attachPaymentSourceToCustomerObject(event, sql):
#     if 'target' not in event: raise RuntimeError("Please specify 'target'.")
#     if 'source' not in event: raise RuntimeError("Please specify 'source'.")
#     # source is the same as token id
#     cust_id = getCustomerIdFromUserName(event["target"], sql)
#     stripe.api_key = stripeconf.api_key
#     customer = stripe.Customer.retrieve(cust_id)
#     return customer.sources.create(source=event["source"])

# # This method is called when the user changes their selected payment method in our UI.
# def selectDefaultPaymentSource(event, sql):
#     if 'target' not in event: raise RuntimeError("Please specify 'target'.")
#     if 'default_source' not in event: raise RuntimeError("Please specify 'default_source'.")
#     cust_id = getCustomerIdFromUserName(event["target"], sql)
#     stripe.api_key = stripeconf.api_key
#     customer = stripe.Customer.retrieve(cust_id)
#     customer.default_source = event["default_source"]
#     status = customer.save()
#     return status

# def createSubscription(event, sql):
#     if 'target' not in event: raise RuntimeError("Please specify 'target'.")
#     #if 'source' not in event: raise RuntimeError("Please specify 'source'.")
#     #if 'amount' not in event: raise RuntimeError("Please specify 'amount'.")
#     #if 'currency' not in event: raise RuntimeError("Please specify 'currency'.")
#     if 'plan' not in event: raise RuntimeError("Please specify 'plan'.")
    
#     cust_id = getCustomerIdFromUserName(event["target"], sql)
#     stripe.api_key = stripeconf.api_key
#     status = stripe.Subscription.create(
#         #amount=event["amount"],
#         #currency=event["currency"],
#         plan=event["plan"],
#         customer=cust_id
#     )
#     return str(status)

# def cancelSubscription(event, sql):
#     if 'target' not in event: raise RuntimeError("Please spcify 'target'.")
#     cust_id = getCustomerIdFromUserName(event["target"], sql)
#     stripe.api_key = stripeconf.api_key
#     customer = stripe.Customer.retrieve(cust_id)
#     subscriptions = stripe.Subscription.list(customer=cust_id)
#     # Getting the first subscribed plan of this user and cancel it
#     # Note that each customer should only have 0 or 1 subscribed plan
#     subscribeID = subscriptions["data"][0]["id"]
#     status = stripe.Subscription.retrieve(subscribeID).delete(at_period_end=True)

#     return str(status)

# def countSubscriptionOfCustomer(event, sql):
#     if 'target' not in event: raise RuntimeError("Please spcify 'target'.")
#     cust_id = getCustomerIdFromUserName(event["target"], sql)
#     stripe.api_key = stripeconf.api_key
#     customer = stripe.Customer.retrieve(cust_id)
#     subscriptions = stripe.Subscription.list(customer=cust_id)
#     return len(subscriptions["data"])


def verifyAppleReceipt(event):
    if 'target' not in event: raise RuntimeError("Please specify 'target'.")
    if 'receipt_json' not in event: raise RuntimeError("Please specify 'receipt_json'.")

    try:
        #with itunesiap.env.current().clone(use_sandbox=True):
        response = itunesiap.verify(event["receipt_json"], itunesiapconf.api_key)
        return response
    except Exception as e:
        print('invalid receipt provided')
        return str(e)

def subscriptionGetExpiresDate(event):
    response = verifyAppleReceipt(event)

    try:
        return response.receipt.last_in_app.expires_date_ms
    except Exception as e:
        return str(e)
        #return 0

dispatch = {
    'adduser':                          adduser,
    'updatern':                         updatern,
    'simplesearch':                     simplesearch,
    'follow':                           follow,
    'unfollow':                         unfollow,
    'followRequest':                    followRequest,
    'unfollowRequest':                  unfollowRequest,
    'getNumFollowers':                  getNumFollowers,
    'getNumFollowees':                  getNumFollowees,
    'getNumFollowRequests':             getNumFollowRequests,
    'getFollowers':                     getFollowers,
    'getFollowees':                     getFollowees,
    'getFollowersDict':                 getFollowersDict,
    'getFolloweesDict':                 getFolloweesDict,
    'getFollowerRequests':              getFollowerRequests,
    'getFolloweeRequests':              getFolloweeRequests,
    'getFollowerRequestsDict':          getFollowerRequestsDict,
    'getFolloweeRequestsDict':          getFolloweeRequestsDict,
    'doIFollow':       	                doIFollow,
    'didISendFollowRequest':            didISendFollowRequest,
    'createScanCodeForUser':            createScanCodeForUser,
    'getUserPageViews':                 getUserPageViews,
    'getUserCodeScans':                 getUserCodeScans,
    'getUserSinglePayViewsForDay':      getUserSinglePayViewsForDay,
    'getUserTotalEngagements':          getUserTotalEngagements,
    'getUserSingleEngagements':         getUserSingleEngagements,
    'getUserTotalEngagementsBreakdown': getUserTotalEngagementsBreakdown,
    'getUserPageViewsLocations':        getUserPageViewsLocations,
    # 'createPaymentCustomer':            createPaymentCustomer,
    # 'getPaymentCustomerObject':         getPaymentCustomerObject,
    # 'attachPaymentSourceToCustomerObject': attachPaymentSourceToCustomerObject,
    # 'selectDefaultPaymentSource':       selectDefaultPaymentSource,
    # 'createSubscription':               createSubscription,
    # 'cancelSubscription':               cancelSubscription,
    # 'countSubscriptionOfCustomer':      countSubscriptionOfCustomer
    'verifyAppleReceipt':               verifyAppleReceipt,
    'subscriptionGetExpiresDate':       subscriptionGetExpiresDate
}

# List all functions that do not need to connect to mysql database
dispatch_sql_not_needed = [
    "createScanCodeForUser",
    "getUserPageViews",
    "getUserCodeScans",
    "getUserSinglePayViewsForDay",
    "getUserTotalEngagements",
    "getUserSingleEngagements",
    "getUserTotalEngagementsBreakdown",
    "getUserPageViewsLocations",
    "verifyAppleReceipt",
    "subscriptionGetExpiresDate"
]


def getCustomerIdFromUserName(userName, sql):
    query = "SELECT customerid FROM users WHERE username = '{target}'".format(
        target = userName 
    )
    return sql_select(sql, query)[0][0]

def handler(event, context):
    if type(event) is not dict: raise RuntimeError("Parameters must be a hash.")
    
    if 'action' not in event: raise RuntimeError("No action specified.")
    action = event['action']
    
    # if 'target' not in event: raise RuntimeError("No target specified.")
    
    if action not in dispatch: raise RuntimeError("Invalid action: " + action)
    delegate = dispatch[action]

    if action not in dispatch_sql_not_needed:
        sql = pymysql.connect(
            sqlconf.endpoint,
            sqlconf.username,
            "",
            sqlconf.dbname
        )
        result = delegate(event, sql)
        sql.close()
    else:
        result = delegate(event)

    return result
