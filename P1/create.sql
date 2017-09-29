drop table if exists Item;
drop table if exists User;
drop table if exists Bids;
drop table if exists Category;

--the correct create order should be user --> Item --> Bids --> Category since each of them are depended on each other

create table User(
	UserID		bigint,
	Country		string,
	Location	string,
	Rating		int,
	PRIMARY KEY (UserID)
) 

create table Item(
	ItemID		bigint,
	Name		string NOT NULL,
	Buy_Price	double,
	First_Bid	double NOT NULL,
	Number_of_Bids	int NOT NULL,
	Started		date NOT NULL, 
	Ends		date NOT NULL,
	Currently	double NOT NULL,
	Seller_ID	bigint NOT NULL,
	Description	string NOT NULL,
	PRIMARY KEY (ItemId),
	FOREIGN KEY (Seller_ID) REFERENCES User(UserID)
)

create table Bids(
	UserID		bigint,
	Time		date NOT NULL,
	Amount		double NOT NULL,
	ItemID		bigint NOT NULL,
	PRIMARY KEY (UserID, Time, Amount, ItemID), --maybe can be reduce to 3
	FOREIGN KEY (UserID) REFERENCES User(UserID),
	FOREIGN KEY (ItemID) REFERENCES Item(ItemID)
)

create table Category(
	Name		string,
	ItemID		bigint,
	PRIMARY KEY (Name, ItemID),
	FOREIGN KEY (ItemID) REFERENCES Item(ItemID)
)

