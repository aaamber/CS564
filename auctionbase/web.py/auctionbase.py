#!/usr/bin/env python

import sys; sys.path.insert(0, 'lib') # this line is necessary for the rest
import os                             # of the imports to work!

import web
import sqlitedb
from jinja2 import Environment, FileSystemLoader
from datetime import datetime

###########################################################################################
##########################DO NOT CHANGE ANYTHING ABOVE THIS LINE!##########################
###########################################################################################

######################BEGIN HELPER METHODS######################

# helper method to convert times from database (which will return a string)
# into datetime objects. This will allow you to compare times correctly (using
# ==, !=, <, >, etc.) instead of lexicographically as strings.

# Sample use:
# current_time = string_to_time(sqlitedb.getTime())

def string_to_time(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

# helper method to render a template in the templates/ directory
#
# `template_name': name of template file to render
#
# `**context': a dictionary of variable names mapped to values
# that is passed to Jinja2's templating engine
#
# See curr_time's `GET' method for sample usage
#
# WARNING: DO NOT CHANGE THIS METHOD
def render_template(template_name, **context):
    extensions = context.pop('extensions', [])
    globals = context.pop('globals', {})

    jinja_env = Environment(autoescape=True,
            loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
            extensions=extensions,
            )
    jinja_env.globals.update(globals)

    web.header('Content-Type','text/html; charset=utf-8', unique=True)

    return jinja_env.get_template(template_name).render(context)

#####################END HELPER METHODS#####################

urls = ('/currtime', 'curr_time',
        '/', 'index',
        '/selecttime', 'select_time',
        '/search', 'search',
        '/timetable', 'timetable',
        '/add_bid', 'addbid',
        '/item', 'show_item'
        )

class index:
    # A simple GET request, to '/currtime'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        #current_time = sqlitedb.getTime()
        return render_template('index.html')


class timetable:
    # A simple GET request, to '/currtime'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        #current_time = sqlitedb.getTime()
        return render_template('timetable.html')

class show_item:
    def GET(self):
        return render_template('item.html', item = None)
    def POST(self):
        post_params = web.input()
        item_id = post_params['item_id']
        # get bids
        bid_result = sqlitedb.getBids(item_id)
        item = sqlitedb.getItemById(item_id)
        categories = sqlitedb.getCategories(item_id)
        current_time = sqlitedb.getTime()
        if item['Buy_Price'] is not None:
            buy_price = item['Buy_Price']
        else:
            buy_price = float('inf')

        winner = None
        # check status
        if item['Ends'] > current_time and item['Currently'] < buy_price:
            open = True
        else:
            if item['Number_of_Bids'] > 0: 
                winner = sqlitedb.getWinner(item_id, item['Currently'])['UserID']
            open = False

        return render_template('item.html', bid_result = bid_result, item = item, categories = categories, open = open, winner = winner)
        

class addbid:
    def GET(self):
        #current_time = sqlitedb.getTime()
        return render_template('add_bid.html')

    def POST(self):
        current_time = sqlitedb.getTime()
        post_params = web.input()
        itemID = post_params['itemID']
        userID = post_params['userID']
        price = post_params['price']

        
        if sqlitedb.isUserValid(userID):
            try:
                if sqlitedb.isBidAlive(itemID):
                    # Add bid into db
                    t = sqlitedb.transaction()
                    try:
                        #sqlitedb.update('update CurrentTime set time = $updated_time', {'updated_time': selected_time})
                        sqlitedb.query('INSERT INTO Bids (itemID, UserID, Amount, Time) VALUES ($itemID, $userid, $price, $time) ', {'itemID': itemID, 'userid': userID, 'price': price, 'time' : current_time }, True)
                    except Exception as e:
                        t.rollback()
                        print str(e)
                        update_message = 'An error occur'
                        result = False
                    else:
                        t.commit()
                        update_message = 'Success!'
                        result = True
                else:
                    update_message = 'Auction on this item is closed'
                    result = False
            except Exception as e:
                # item not exist
                print(e)
                result = False
                update_message = 'Item not exist'
        else:
            result = False
            update_message = 'User is not exist'

        return render_template('add_bid.html', add_result = result, message = update_message)

class search:
    # A simple GET request, to '/currtime'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        #current_time = sqlitedb.getTime()
        return render_template('search.html')
    def POST(self):
        post_params = web.input()
        itemID = post_params['itemID']
        itemCategory = post_params['itemCategory']
        itemDescription = post_params['itemDescription']
        userID = post_params['userID']
        minPrice = post_params['minPrice']
        maxPrice = post_params['maxPrice']
        status = post_params['status']

        userQuery = {'itemID':itemID,'itemCategory':itemCategory,'itemDescription':itemDescription,
                        'userID':userID,'minPrice':minPrice,'maxPrice':maxPrice,'status':status}
        
        searchQuery = {}        
        searchSentenceDict = {'select':['distinct','*'],'from':[],'where':[]}
        searchSentence = ""

        #Get the query information that user added, save that in the searchQuery dictionary for variable passing.
        #Add the necessary syntax to execute those querys into the searchSentenceDict.
        for key,value in userQuery.items():
            #check to see if the value is not empty
            if value != "":
                #Translations for each into the search sentence
                #ItemID query
                if "itemID" == key:
                    #check if item is valid
                    if not sqlitedb.isItemValid(value):
                        #if the item is not valid return html itemID is wrong
                            error_message = 'Item ID:' + value + ' does not exist'
                            return render_template('search.html',message = error_message)
                    if "Items" not in searchSentenceDict['from']:
                        searchSentenceDict['from'].append('Items')
                    searchSentenceDict['where'].append('Items.itemID = $itemID')
                #ItemCategory query
                elif "itemCategory" == key:
                    #check if category is valid
                    if not sqlitedb.isCategoryValid(value):
                        #if the item is not valid return html itemID is wrong
                            error_message = 'Item Category:' + value + ' does not exist'
                            return render_template('search.html',message = error_message)
                    #Nested EXISTS in WHERE will work as intended requries Items to be in above FROM
                    if "Items" not in searchSentenceDict['from']:
                        searchSentenceDict['from'].append('Items')
                    #return all items matching the itemID (does all if no specific itemID) and the specified category
                    searchSentenceDict['where'].append('exists(select Categories.itemID from Categories where Categories.category = $itemCategory and Categories.itemID = Items.itemID)')
                #ItemDescription query
                elif "itemDescription" == key:
                    if "Items" not in searchSentenceDict['from']:
                        searchSentenceDict['from'].append('Items')
                    searchSentenceDict['where'].append('Items.Description like $itemDescription')
                    #Adjust the value so it will execute a pattern match on the string
                    value = '%' + value + '%'
                #UserID querey
                elif "userID" == key:
                    #check if user is valid
                    if not sqlitedb.isUserValid(value):
                        #if the user is not valid return html userID is wrong
                            error_message = 'User ID:' + value + ' does not exist'
                            return render_template('search.html',message = error_message)
                    #Nested Exists Similar to itemCategory, requires Items to be in above FROM
                    if "Items" not in searchSentenceDict['from']:
                        searchSentenceDict['from'].append('Items')
                    #return all items matching the userID. 
                    searchSentenceDict['where'].append('exists(select Users.userID from Users where Users.userID = $userID and Items.Seller_UserID==Users.userID)')
                #MinPrice query
                elif "minPrice" == key:
                     #check if user is valid
                    if not sqlitedb.isMinPriceValid(value):
                        #if the user is not valid return html userID is wrong
                            error_message = 'An item does not exist with a Min Price:' + value
                            return render_template('search.html',message = error_message)
                    if "Items" not in searchSentenceDict['from']:
                        searchSentenceDict['from'].append('Items')
                    #return all items whose "Currently" price is greater than or equal too minPrice.
                    searchSentenceDict['where'].append('Items.Currently >= $minPrice')
                #MinPrice query
                elif "maxPrice" == key:
                    if not sqlitedb.isMaxPriceValid(value):
                        #if the user is not valid return html userID is wrong
                            error_message = 'An item does not exist with a Max Price:' + value
                            return render_template('search.html',message = error_message)
                    if "Items" not in searchSentenceDict['from']:
                        searchSentenceDict['from'].append('Items')
                    #return all items whose "Currently" price is greater than or equal too minPrice.
                    searchSentenceDict['where'].append('Items.Currently <= $maxPrice')
                #Status query
                elif "status" == key:
                    #get the current time of the system
                    currentTime=sqlitedb.getTime()
                    currentTimeString = '\''+currentTime+'\''
                    if not sqlitedb.isStatusValid(value,currentTimeString):
                        #if the user is not valid return html userID is wrong
                            error_message = 'An item does not exist with Status:' + value
                            return render_template('search.html',message = error_message)

                    if "Items" not in searchSentenceDict['from']: 
                        searchSentenceDict['from'].append('Items')
                    
                    #if we are looking for open items, check for the ending time greater than current
                    if value == 'open':
                        searchSentenceDict['where'].append('Items.Ends>' + currentTimeString +' and Currently<Buy_Price')
                    #if we are looking for closed items, check for the ending time less than current
                    elif value == 'close':
                        searchSentenceDict['where'].append('(Items.Ends<' + currentTimeString + ' or Currently>=Buy_Price)')
                    #if we are looking for not started items, check for the starting time greater than current
                    elif value == 'notStarted':
                        searchSentenceDict['where'].append('Items.Started>' + currentTimeString)
                    #otherwise all is the value, thus add no where clause
                    else:
                        pass

                #Finally add the value to the official searchQuery 
                searchQuery[key] = value                     
            else:
                print(key + "was empty")
        
        #Contructs the querey searchSentence for db query from the searchSentenceDict
        #SELECT
        searchSentence = 'select'
        for index,value in enumerate(searchSentenceDict['select']):
            if(index+1 != len(searchSentenceDict['from'])):
                searchSentence = searchSentence + ' ' + value
            else:
                searchSentence = searchSentence + ' ' + value
        #FROM
        if len(searchSentenceDict['from'])!=0:
            searchSentence = searchSentence + ' from'
            for index,value in enumerate(searchSentenceDict['from']):
                if(index+1 != len(searchSentenceDict['from'])):
                    searchSentence = searchSentence + ' ' + value + ','
                else:
                    searchSentence = searchSentence + ' ' + value
        #WHERE
        if len(searchSentenceDict['where'])!=0:
            searchSentence = searchSentence + ' where'
            for index,value in enumerate(searchSentenceDict['where']):
                if(index+1 != len(searchSentenceDict['where'])):
                    searchSentence = searchSentence + ' ' + value + ' and'
                else:
                    searchSentence = searchSentence + ' ' + value

        
        #DEBUG print messages
        #print(searchQuery)
        #print(searchSentence)
        #searchSentence=searchSentence + ' limit 10'
        #Create the transcation,query the db, return the search results based off query,update html.
        t = sqlitedb.transaction()
        try:
            search_result = sqlitedb.query(searchSentence, searchQuery)

        except Exception as e:
            t.rollback()
            print str(e)
        else:
            t.commit()
        return render_template('search.html',search_result = search_result, search_params=userQuery)
        

class curr_time:
    # A simple GET request, to '/currtime'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        current_time = sqlitedb.getTime()
        return render_template('curr_time.html', time = current_time)

class select_time:
    # Aanother GET request, this time to the URL '/selecttime'
    def GET(self):
        return render_template('select_time.html')

    # A POST request
    #
    # You can fetch the parameters passed to the URL
    # by calling `web.input()' for **both** POST requests
    # and GET requests
    def POST(self):
        post_params = web.input()
        MM = post_params['MM']
        dd = post_params['dd']
        yyyy = post_params['yyyy']
        HH = post_params['HH']
        mm = post_params['mm']
        ss = post_params['ss'];
        enter_name = post_params['entername']


        selected_time = '%s-%s-%s %s:%s:%s' % (yyyy, MM, dd, HH, mm, ss)
        update_message = '(Hello, %s. Previously selected time was: %s.)' % (enter_name, selected_time)
        # TODO: save the selected time as the current time in the database
        sqlitedb.updateTime(selected_time)

        # Here, we assign `update_message' to `message', which means
        # we'll refer to it in our template as `message'
        return render_template('select_time.html', message = update_message)

###########################################################################################
##########################DO NOT CHANGE ANYTHING BELOW THIS LINE!##########################
###########################################################################################

if __name__ == '__main__':
    web.internalerror = web.debugerror
    app = web.application(urls, globals())
    app.add_processor(web.loadhook(sqlitedb.enforceForeignKey))
    app.run()