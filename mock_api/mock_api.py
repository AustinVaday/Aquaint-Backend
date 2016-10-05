import pymysql, sqlconf

def sql_select(sql, query, inserts=()):
    cursor = sql.cursor()
    cursor.execute(query, inserts)
    results = cursor.fetchall()
    return results

def sql_cd(sql, query, inserts=()):
    cursor = sql.cursor()
    count = cursor.execute(query, inserts)
    sql.commit()
    return count


def adduser(event, sql):
    if 'realname' not in event: raise RuntimeError("Please specify 'realname'.")
    
    query = 'INSERT INTO users (username, realname) VALUES(%s, %s);'
    inserts = (event['target'], event['realname'])
    
    return sql_cd(sql, query, inserts)

def updatern(event, sql):
    if 'realname' not in event: raise RuntimeError("Please specify 'realname'.")
    
    query = 'UPDATE users SET realname = %s WHERE username = %s;'
    inserts = (event['realname'], event['target'])
    
    return sql_cd(sql, query, inserts)

def simplesearch(event, sql):
    if 'start' not in event or 'end' not in event:
        raise RuntimeError("Please specify range 'start' and 'end'.")
    
    sqlstart = max(event['start'], 0)
    sqlcount = max(sqlstart, event['end']) - sqlstart
    matcher = '%{}%'.format(event['target'])
    
    query = ("SELECT username FROM users WHERE username LIKE %s " + \
        "OR realname LIKE %s LIMIT %s, %s;")
    inserts = (matcher, matcher, sqlstart, sqlcount)
    
    result = sql_select(sql, query, inserts)
    
    return list(map(lambda x: x[0], result))


def follow(event, sql):
    if 'me' not in event: raise RuntimeError("Please specify 'me'.")
    
    query = "INSERT INTO follows (follower, followee) VALUES (" + \
            "(SELECT user_index FROM users WHERE username = %s), " + \
            "(SELECT user_index FROM users WHERE username = %s) " + \
        ");"
    inserts = (event['me'], event['target'])
    
    return sql_cd(sql, query)

def unfollow(event, sql):
    if 'me' not in event: raise RuntimeError("Please specify 'me'.")

    query = "DELETE FROM follows " + \
        "WHERE follower = (" + \
            "SELECT user_index FROM users WHERE username = %s" + \
        ") AND followee = (" + \
            "SELECT user_index FROM users WHERE username = %s" + \
        ");"
    inserts = (event['me'], event['target'])

    return sql_cd(sql, query, inserts)


def getNumFollowers(event, sql):
    query = "SELECT COUNT(follower) FROM username_follows " + \
        "WHERE followee = %s;"
    inserts = (event['target'])
    
    return sql_select(sql, query, inserts)[0][0]


def getNumFollowees(event, sql):
    query = "SELECT COUNT(followee) FROM username_follows WHERE follower = %s;"
    inserts = (event['target'])
    
    return sql_select(sql, query, inserts)[0][0]


def getFollowers(event, sql):
    query = "SELECT follower, UNIX_TIMESTAMP(timestamp) " + \
        "FROM username_follows WHERE followee = %s ORDER BY timestamp DESC;"
    inserts = (event['target'])
    
    return sql_select(sql, query, inserts)

def getFollowees(event, sql):
    query = "SELECT followee, UNIX_TIMESTAMP(timestamp) " + \
        "FROM username_follows WHERE follower = %s ORDER BY timestamp DESC;"
    inserts = (event['target'])
    
    return sql_select(sql, query, inserts)

def getFollowersDict(event, sql):
    return dict(getFollowers(event,sql))

def getFolloweesDict(event, sql):
    return dict(getFollowees(event,sql))

dispatch = {
    'adduser':          adduser,
    'updatern':         updatern,
    'simplesearch':     simplesearch,
    'follow':           follow,
    'unfollow':         unfollow,
    'getNumFollowers':  getNumFollowers,
    'getNumFollowees':  getNumFollowees,
    'getFollowers':     getFollowers,
    'getFollowees':     getFollowees,
    'getFollowersDict': getFollowersDict,
    'getFolloweesDict': getFolloweesDict
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
