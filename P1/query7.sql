SELECT COUNT(*) FROM (SELECT DISTINCT Name FROM (SELECT ItemID as ID FROM Items WHERE Currently>100 and Number_of_Bids<>0),Category WHERE ID=ItemID);