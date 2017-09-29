
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

def escapeQuote(description):
    count = 0
    for i, c in enumerate(description):
        if c == "\"":
            count+=1
            description = description[:(i+count)]+"\""+description[(i+count):]
    description = "\""+description+"\""
    return description
    

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        
        #bids = items['Bids']
        itemTable = open("itemTable.dat","a")
        bidTable = open("bidTable.dat","a")
        userTable = open("userTable.dat","a")
        categoryTable = open("categoryTable.dat","a")
        userIDs = []
        itemIDs = []
        #print len(items)
        bidCount = 0
        for item in items:
            if item["ItemID"] not in itemIDs:
                itemIDs.append(item["ItemID"])
                for key, value in item.iteritems():
                    if key == "Seller":
                        #add to userTable only if the user doesn't exist
                        if value['UserID'] not in userIDs:
                            userIDs.append(value['UserID'])
                            userTable.write(str(value["UserID"])+"|"+value["Rating"]+"|"+escapeQuote(item["Location"])
                                            +"|"+escapeQuote(item["Country"]) +"\n")
                    elif key  == "Category":
                        for category in value:
                            categoryTable.write(str(item["ItemID"])+ "|" + category+"\n")
                    #skip empty Bids
                    elif key == "Bids":
                        if value: 
                            bidCount += len(value)
                            for bid in value:
                                bidTime = transformDttm(bid["Bid"]["Time"])
                                bidAmount = transformDollar(bid["Bid"]["Amount"])
                                #add userID and itemID
                                bidUserId = bid["Bid"]["Bidder"]['UserID']
                                bidTable.write(bidUserId+"|"+str(item["ItemID"])
                                               +"|"+bidTime+"|"+bidAmount+"\n")
                                #add to user table
                                if bidUserId not in userIDs:
                                    if "Location" not in bid["Bid"]["Bidder"].keys():
                                        location = "NULL"
                                    else:
                                        location = bid["Bid"]["Bidder"]["Location"]
                                        location = escapeQuote(location)
                                    if "Country" not in bid["Bid"]["Bidder"].keys():
                                        country = "NULL"
                                    else:
                                        country = bid["Bid"]["Bidder"]["Country"]
                                        country = escapeQuote(country)
                                   
                                    userTable.write(bidUserId+"|"+bid["Bid"]["Bidder"]["Rating"]+"|"+location+"|"+country+"\n")
                    elif key in {"Location", "Country", "Buy_Price"}:
                        pass
                    else:
                        if key in {"Currently", "First_Bid"}:
                            value = transformDollar(value)
                        if key in {"Started", "Ends"}:
                            value = transformDttm(value)
                        if isinstance(value, basestring):
                            value = escapeQuote(value)
                        '''
                        if key == "Ends":
                            value += "Ends"
                        if key == "First_Bid":
                            value += "First_Bid"
                        if key == "Currently":
                            value += "Currently"
                        if key == "Number_of_Bids":
                            value += "NumerBids"
                        '''
                        itemTable.write(str(value)+"|")
                        
                    pass
                # User_ID+Buy_price, last two columns
                if "Buy_Price" not in item.keys():
                    itemTable.write(str(item["Seller"]['UserID'])+"|NULL"+"\n")
                else:
                    itemTable.write(str(item["Seller"]['UserID'])+"|"+transformDollar(item["Buy_Price"])+"\n")
            
            """
            TODO: traverse the items dictionary to extract information from the
            given `json_file' and generate the necessary .dat files to generate
            the SQL tables based on your relation design
            """
            pass
        #print bidCount
        itemTable.close()
        bidTable.close()
        userTable.close()
        categoryTable.close()

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print "Success parsing " + f

if __name__ == '__main__':
    main(sys.argv)
