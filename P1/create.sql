DROP TABLE IF EXISTS Items;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Bids;
DROP TABLE IF EXISTS Category;

--the correct create order should be users --> Items --> Bids --> Category since each of them are depended on each other

create table Users(
	Rating		INT,
	UserID		VARCHAR,
	Location	VARCHAR,
	Country		VARCHAR,
	PRIMARY KEY (UserID)
); 

create table Items(
	ItemID		FLOAT,
	Name		VARCHAR NOT NULL,
	Buy_Price	DOUBLE,
	First_Bid	DOUBLE NOT NULL,
	Number_of_Bids	INT NOT NULL,
	Started		DATE NOT NULL, 
	Ends		DATE NOT NULL,
	Currently	DOUBLE NOT NULL,
	Seller_ID	VARCHAR NOT NULL,
	Description	VARCHAR NOT NULL,
	PRIMARY KEY (ItemId),
	FOREIGN KEY (Seller_ID) REFERENCES Users(UserID)
);

create table Bids(
	UserID		VARCHAR,
	Time		DATE NOT NULL,
	Amount		VARCHAR NOT NULL,
	ItemID		FLOAT NOT NULL,
	PRIMARY KEY (UserID, Time, Amount, ItemID), --maybe can be reduce to 3
	FOREIGN KEY (UserID) REFERENCES Users(UserID),
	FOREIGN KEY (ItemID) REFERENCES Items(ItemID)
);

create table Category(
	Name		VARCHAR,
	ItemID		FLOAT,
	PRIMARY KEY (Name, ItemID),
	FOREIGN KEY (ItemID) REFERENCES Items(ItemID)
);

