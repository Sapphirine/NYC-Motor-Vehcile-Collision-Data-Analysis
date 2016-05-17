import os
import sys
import math
import csv

# Path for spark source folder
os.environ['SPARK_HOME']="/Users/anshuman/Downloads/spark-1.6.0/"

# Appending pyspark  to Python Path
sys.path.append("/Users/anshuman/Downloads/spark-1.6.0/python")


from pyspark import SparkContext
from pyspark.sql import SQLContext
from collections import OrderedDict
from collections import defaultdict
from operator import itemgetter
import pandas as pd

sc = SparkContext(appName = "Loading Data") # if using locally
sql_sc = SQLContext(sc)

pandas_df = pd.read_csv('Collision_Data2.csv')  # assuming the file contains a header
# pandas_df = pd.read_csv('file.csv', names = ['column 1','column 2']) # if no header
s_df = sql_sc.createDataFrame(pandas_df)

#s_df.where($"time" like )
#s_df.printSchema()


factormap = {}
s_df.registerTempTable("s_df");

df1 = sql_sc.sql("select DATE,COUNT(*) as count from s_df group By DATE order by count desc")
pdf = df1.toPandas();
for index, row in pdf.iterrows():
	if row['DATE'] in factormap:
		factormap[row['DATE']] = factormap[row['DATE']] + row['count'];
	else:
		factormap[row['DATE']] = row['count'];

writer = csv.writer(open('dangerousdates.csv', 'wb'))
writer.writerow(["date", "count"])
for key, value in factormap.items():
   	writer.writerow([key, value])



count = 0;
dates_dict = defaultdict(list)
for x in range(0,24):
	df1 = sql_sc.sql("select BOROUGH,COUNT(*) as timeCount from s_df where TIME like '" + str(x) + ":%' group By BOROUGH")
	pdf = df1.toPandas()
	for index,row in pdf.iterrows():
		count = count + 1
		dates_dict[row['BOROUGH']].append(row['timeCount'])
print "Count is ", count;
del dates_dict['NaN']
# del dates_dict['nan']
# del dates_dict['Unspecified']

writer = csv.writer(open('boroughtimes.csv', 'wb')) 
writer.writerow(dates_dict.keys())
writer.writerows(zip(*[dates_dict[key] for key in dates_dict.keys()]))

livesDict = []
for x in range(0,24):
	df1 = sql_sc.sql("select COUNT(*) as deathCount from s_df where TIME like '" + str(x) + ":%' and NUMBEROFPERSONSKILLED > 0")
	pdf = df1.toPandas()
	for index,row in pdf.iterrows():
		livesDict.append(row['deathCount'])


writer = csv.writer(open('deathsbytime.csv', 'wb')) 
writer.writerow(["Number of deaths"])
for value in livesDict:
	writer.writerow([value])

locationDict = {}

df1 = sql_sc.sql("select LATITUDE,LONGITUDE from s_df where NUMBEROFPERSONSKILLED > 0")
pdf = df1.toPandas();
for index, row in pdf.iterrows():
	a = str(row['LATITUDE']) + str(row['LONGITUDE'])
	if a in locationDict:
		locationDict[a] = locationDict[a] + 1;
	else:
		locationDict[a] = 1;

for key,value in locationDict.items():
	if value > 1:
		print "Key is ", key, "\t Value is ", value


del locationDict['nannan']
writer = csv.writer(open('locations.csv', 'wb'))
writer.writerow(["lat", "lng"])
for key, value in locationDict.items():
   if value > 1:
   		lat = key.split("-")[0]
   		lng = "-" + key.split("-")[1]
   		print "Lat is ", lat, " Lng is ", lng
   		writer.writerow([lat, lng])

location1Dict = {}

df1 = sql_sc.sql("select LATITUDE,LONGITUDE from s_df")
pdf = df1.toPandas();
for index, row in pdf.iterrows():
	a = str(row['LATITUDE']) + str(row['LONGITUDE'])
	if a in location1Dict:
		location1Dict[a] = location1Dict[a] + 1;
	else:
		location1Dict[a] = 1;

del location1Dict['nannan']
writer = csv.writer(open('locationaccidents.csv', 'wb'))
writer.writerow(["lat", "lng"])
for key, value in location1Dict.items():
   if value > 1:
   		lat = key.split("-")[0]
   		lng = "-" + key.split("-")[1]
   		print "Lat is ", lat, " Lng is ", lng
   		writer.writerow([lat, lng])


factormap = {}

df1 = sql_sc.sql("select CONTRIBUTINGFACTORVEHICLE1,COUNT(*) as count from s_df group By CONTRIBUTINGFACTORVEHICLE1")
pdf = df1.toPandas();
for index, row in pdf.iterrows():
	if row['CONTRIBUTINGFACTORVEHICLE1'] in factormap:
		factormap[row['CONTRIBUTINGFACTORVEHICLE1']] = factormap[row['CONTRIBUTINGFACTORVEHICLE1']] + row['count'];
	else:
		factormap[row['CONTRIBUTINGFACTORVEHICLE1']] = row['count'];



df2 = sql_sc.sql("select CONTRIBUTINGFACTORVEHICLE2,COUNT(*) as count from s_df group By CONTRIBUTINGFACTORVEHICLE2")
pdf = df2.toPandas();
for index, row in pdf.iterrows():
	if row['CONTRIBUTINGFACTORVEHICLE2'] in factormap:
		factormap[row['CONTRIBUTINGFACTORVEHICLE2']] = factormap[row['CONTRIBUTINGFACTORVEHICLE2']] + row['count'];
	else:
		factormap[row['CONTRIBUTINGFACTORVEHICLE2']] = row['count'];




df3 = sql_sc.sql("select CONTRIBUTINGFACTORVEHICLE3,COUNT(*) as count from s_df group By CONTRIBUTINGFACTORVEHICLE3")
pdf = df3.toPandas();
for index, row in pdf.iterrows():
	if row['CONTRIBUTINGFACTORVEHICLE3'] in factormap:
		factormap[row['CONTRIBUTINGFACTORVEHICLE3']] = factormap[row['CONTRIBUTINGFACTORVEHICLE3']] + row['count'];
	else:
		factormap[row['CONTRIBUTINGFACTORVEHICLE3']] = row['count'];



df4 = sql_sc.sql("select CONTRIBUTINGFACTORVEHICLE4,COUNT(*) as count from s_df group By CONTRIBUTINGFACTORVEHICLE4")
pdf = df4.toPandas();
for index, row in pdf.iterrows():
	if row['CONTRIBUTINGFACTORVEHICLE4'] in factormap:
		factormap[row['CONTRIBUTINGFACTORVEHICLE4']] = factormap[row['CONTRIBUTINGFACTORVEHICLE4']] + row['count'];
	else:
		factormap[row['CONTRIBUTINGFACTORVEHICLE4']] = row['count'];

# print factormap

d = OrderedDict(sorted(factormap.items(), key = itemgetter(1), reverse = True))
print d



writer = csv.writer(open('factor.csv', 'wb'))
for key, value in d.items():
   if not (str(key).lower() == "nan" or str(key).lower() == "unspecified"):
   		writer.writerow([key, value])

vehiclemap = {}


df1 = sql_sc.sql("select VEHICLETYPECODE1,COUNT(*) as count from s_df group By VEHICLETYPECODE1")
pdf = df1.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPECODE1'] in vehiclemap:
		vehiclemap[row['VEHICLETYPECODE1']] = vehiclemap[row['VEHICLETYPECODE1']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPECODE1']] = row['count'];




df2 = sql_sc.sql("select VEHICLETYPECODE2,COUNT(*) as count from s_df group By VEHICLETYPECODE2")
pdf = df2.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPECODE2'] in vehiclemap:
		vehiclemap[row['VEHICLETYPECODE2']] = vehiclemap[row['VEHICLETYPECODE2']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPECODE2']] = row['count'];



df3 = sql_sc.sql("select VEHICLETYPECODE3,COUNT(*) as count from s_df group By VEHICLETYPECODE3")
pdf = df3.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPECODE3'] in vehiclemap:
		vehiclemap[row['VEHICLETYPECODE3']] = vehiclemap[row['VEHICLETYPECODE3']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPECODE3']] = row['count'];




df4 = sql_sc.sql("select VEHICLETYPECODE4,COUNT(*) as count from s_df group By VEHICLETYPECODE4")
pdf = df4.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPECODE4'] in vehiclemap:
		vehiclemap[row['VEHICLETYPECODE4']] = vehiclemap[row['VEHICLETYPECODE4']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPECODE4']] = row['count'];



df5 = sql_sc.sql("select VEHICLETYPECODE5,COUNT(*) as count from s_df group By VEHICLETYPECODE5")
pdf = df5.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPECODE5'] in vehiclemap:
		vehiclemap[row['VEHICLETYPECODE5']] = vehiclemap[row['VEHICLETYPECODE5']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPECODE5']] = row['count'];


d = OrderedDict(sorted(vehiclemap.items(), key = itemgetter(1), reverse = True))
# print d



writer = csv.writer(open('vehicle.csv', 'wb'))
for key, value in d.items():
   if not (str(key).lower() == "nan" or str(key).lower() == "unspecified"):
   		writer.writerow([key, value])



To group by location, we can join lat and long columns and then do a group by on then
df5 = sql_sc.sql("select df1.`CONTRIBUTING FACTOR`, df1.count + df2.count + df3.count FROM df1 left outer join df2 on df1.`CONTRIBUTING FACTOR` = df2.`CONTRIBUTING FACTOR` left outer join df3 on df2.`CONTRIBUTING FACTOR`=df3.`CONTRIBUTING FACTOR` ");
df5.show();


s_df.filter(s_df['TIME'] == '2:40').show()




s_df.filter("TIME" like "23:%%").show()
s_df.select("BOROUGH").show()


s_df.groupBy("BOROUGH").count().show()
