rm *.dat
python parser.py ebay_data/items-*.json
sort categoryTable.dat | uniq > categoryT.dat
sort itemTable.dat | uniq > itemT.dat
sort userTable.dat | uniq > userT.dat
sort bidTable.dat | uniq > bidT.dat
rm *Table.dat

