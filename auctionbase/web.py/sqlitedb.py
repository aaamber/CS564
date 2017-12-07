import web

db = web.database(dbn='sqlite',
        db='AuctionBase'
    )

######################BEGIN HELPER METHODS######################

# Enforce foreign key constraints
# WARNING: DO NOT REMOVE THIS!
def enforceForeignKey():
    db.query('PRAGMA foreign_keys = ON')

# initiates a transaction on the database
def transaction():
    return db.transaction()
# Sample usage (in auctionbase.py):
#
# t = sqlitedb.transaction()
# try:
#     sqlitedb.query('[FIRST QUERY STATEMENT]')
#     sqlitedb.query('[SECOND QUERY STATEMENT]')
# except Exception as e:
#     t.rollback()
#     print str(e)
# else:
#     t.commit()
# #
# check out http://webpy.org/cookbook/transactions for examples



# returns the current time from your database
def getTime():
    # TODO: update the query string to match
    # the correct column and table name in your database
    query_string = 'select time as time from CurrentTime'
    results = query(query_string)
    # alternatively: return results[0]['currenttime']
    return results[0].time # TODO: update this as well to match the
                                  # column name

# returns a single item specified by the Item's ID in the database
# Note: if the `result' list is empty (i.e. there are no items for a
# a given ID), this will throw an Exception!
def getItemById(item_id):
    # TODO: rewrite this method to catch the Exception in case `result' is empty
    query_string = 'select * from Items where ItemID = $itemID'
    result = query(query_string, {'itemID': item_id})
    return result[0]

# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars = {}, update = False):
    if update:
        db.query(query_string, vars)
    else:
        return list(db.query(query_string, vars))

#####################END HELPER METHODS#####################

#TODO: additional methods to interact with your database,
# e.g. to update the current time
def isBidAlive(item_id):
    item = getItemById(item_id)
    cur_time = getTime()
    start_time = item.Started
    end_time = item.Ends
    buy_price = item.Buy_Price
    cur_bid_price = item.Currently
    # if start_time <= cur_time:
    #     print('bigger than strat time')
    # if end_time >= cur_time:
    #     print('smaller than end time')
    # if cur_bid_price < buy_price:
    #     print('not buy out yet')
    # exist = (start_time <= cur_time and end_time >= cur_time and cur_bid_price < buy_price)
    return (start_time <= cur_time and end_time >= cur_time and cur_bid_price < buy_price)
