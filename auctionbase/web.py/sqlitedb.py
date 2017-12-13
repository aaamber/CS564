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

def updateTime(selected_time):
    t = db.transaction()
    try:
        #sqlitedb.update('update CurrentTime set time = $updated_time', {'updated_time': selected_time})
        query('update CurrentTime set time = $updated_time', {'updated_time': selected_time}, True)
    except Exception as e:
        t.rollback()
        print str(e)
    else:
        t.commit()

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
    try:
        result[0]
        return result[0]
    except:
        return None


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
    
    priceCheck = False
    if(buy_price==None):
        priceCheck =True
    elif(cur_bid_price<buy_price):
        priceCheck = True

    #DEBUG print
    print(buy_price)
    print(priceCheck)
    print(start_time <= cur_time)
    print(cur_time)
    print(end_time)
    print(end_time >= cur_time)
    # if start_time <= cur_time:
    #     print('bigger than strat time')
    # if end_time >= cur_time:
    #     print('smaller than end time')
    # if cur_bid_price < buy_price:
    #     print('not buy out yet')
    # exist = (start_time <= cur_time and end_time >= cur_time and cur_bid_price < buy_price)
    return (start_time <= cur_time and end_time >= cur_time and priceCheck)

def isUserValid(user_id):
    try:
        query_string = 'select * from Users where UserID = $user_id'
        result = query(query_string, {'user_id': user_id})
        test = result[0]
        return True
    except Exception as e:
        return False

def isItemValid(item_id):
    try:
        query_string = 'select * from Items where ItemID = $item_id limit 1'
        result = query(query_string, {'item_id': item_id})
        test = result[0]
        return True
    except Exception as e:
        return False

def isCategoryValid(category):
    try:
        query_string = 'select * from Categories where Category = $category limit 1'
        result = query(query_string, {'category': category})
        test = result[0]
        return True
    except Exception as e:
        return False

def isMinPriceValid(minPrice):
    try:
        query_string = 'select * from Items where Items.Currently >=$minPrice limit 1'
        result = query(query_string, {'minPrice': minPrice})
        test = result[0]
        return True
    except Exception as e:
        return False

def isMaxPriceValid(maxPrice):
    try:
        query_string = 'select * from Items where Items.Currently <=$maxPrice limit 1'
        result = query(query_string, {'maxPrice': maxPrice})
        test = result[0]
        return True
    except Exception as e:
        return False


def isStatusValid(value,currentTimeString):
    try:
        #if we are looking for open items, check for the ending time greater than current
        if value == 'open':
            query_string = 'select * from Items where Items.Ends>'+currentTimeString + ' and Currently<Buy_Price limit 1'
        #if we are looking for closed items, check for the ending time less than current
        elif value == 'close':
            query_string = 'select * from Items where Items.Ends<'+currentTimeString + ' and Currently>=Buy_Price limit 1'    
        #if we are looking for not started items, check for the starting time greater than current
        elif value == 'notStarted':
            query_string = 'select * from Items where Items.Started>'+currentTimeString + ' limit 1'
        #otherwise all is the value, thus add no where clause
        else:
            return True
        result = query(query_string)
        test = result[0]
        return True
    except Exception as e:
        return False