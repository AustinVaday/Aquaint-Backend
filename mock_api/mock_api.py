import pymysql, sqlconf

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

dispatch = {
    'adduser':                  adduser,
    'updatern':                 updatern,
    'simplesearch':             simplesearch,
    'follow':                   follow,
    'unfollow':                 unfollow,
    'followRequest':            followRequest,
    'unfollowRequest':          unfollowRequest,
    'getNumFollowers':          getNumFollowers,
    'getNumFollowees':          getNumFollowees,
    'getNumFollowRequests':     getNumFollowRequests,
    'getFollowers':             getFollowers,
    'getFollowees':             getFollowees,
    'getFollowersDict':         getFollowersDict,
    'getFolloweesDict':         getFolloweesDict,
    'getFollowerRequests':      getFollowerRequests,
    'getFolloweeRequests':      getFolloweeRequests,
    'getFollowerRequestsDict':  getFollowerRequestsDict,
    'getFolloweeRequestsDict':  getFolloweeRequestsDict,
    'doIFollow':       	        doIFollow,
    'didISendFollowRequest':    didISendFollowRequest
}


def handler(event, context):
    if type(event) is not dict: raise RuntimeError("Parameters must be a hash.")
    
    if 'action' not in event: raise RuntimeError("No action specified.")
    action = event['action']
    
    if 'target' not in event: raise RuntimeError("No target specified.")
    
    if action not in dispatch: raise RuntimeError("Invalid action: " + action)
    delegate = dispatch[action]
    
    sql = pymysql.connect(
        sqlconf.endpoint,
        sqlconf.username,
        "",
        sqlconf.dbname
    )
    
    result = delegate(event, sql)
    
    sql.close()

    return result
