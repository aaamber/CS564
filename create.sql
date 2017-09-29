
# coding: utf-8

# In[ ]:

drop table if exists Items;
drop table if exists Users;
drop table if exists Bids;
drop table if exists Category;

--the correct create order should be users --> Items --> Bids --> Category since each of them are depended on each other

create table Users(
	UserID		FLOAT,
    Rating		INT,
    Location	VARCHAR,
	Country		VARCHAR,
	PRIMARY KEY (UserID)
) 

create table Items(
	ItemID		FLOAT,
    Ends		DATE NOT NULL,
    First_Bid	DOUBLE NOT NULL,
	Name		VARCHAR NOT NULL,
    Started		DATE NOT NULL, 
	Number_of_Bids	INT NOT NULL,
	Currently	DOUBLE NOT NULL,
    Description	VARCHAR NOT NULL,
	Seller_ID	FLOAT NOT NULL,
    Buy_Price	DOUBLE,
	PRIMARY KEY (ItemId),
	FOREIGN KEY (Seller_ID) REFERENCES Users(UserID)
)

create table Bids(
	UserID		FLOAT,
    ItemID		FLOAT NOT NULL,
	Time		DATE NOT NULL,
	Amount		VARCHAR NOT NULL,
	PRIMARY KEY (UserID, Time, Amount, ItemID), --maybe can be reduce to 3
	FOREIGN KEY (UserID) REFERENCES Users(UserID),
	FOREIGN KEY (ItemID) REFERENCES Items(ItemID)
)

create table Category(
    ItemID		FLOAT,
	Name		VARCHAR,
	PRIMARY KEY (Name, ItemID),
	FOREIGN KEY (ItemID) REFERENCES Items(ItemID)
)

