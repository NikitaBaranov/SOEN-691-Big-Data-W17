#!/usr/bin/env python

import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql import Row
from pyspark.sql.types import *


conf = SparkConf().setAppName("NikitaBaranov.Project:CSV Crolling").setMaster("local")
sc = SparkContext(conf=conf)

# input_text_file=sys.argv[1]
# output_text_file=sys.argv[2]

sqlContext = SQLContext(sc)

input_text_file = "/Users/Nikita/hadoop/project_py/data/fakeData.txt"
output_text_file = "/Users/Nikita/hadoop/project_py/out"

df = sqlContext.read.format("com.databricks.spark.csv"). \
    option("header", "true"). \
    option("inferschema", "true"). \
    option("mode", "DROPMALFORMED"). \
    option("delimiter", "\t"). \
    load(input_text_file)

print df.collect()
print df.columns


def f(x):
    print x


df.foreach(f)

# for item in df.columns:
#     for x in list(df.select(item).dropDuplicates()):
#         print x

# Working, create pairs column name : unique values
columns_to_process = ["City", "Region", "product"]
columns_list = list()
new_columns = set()
for item in df.columns:
    if item in columns_to_process:
        for value in list(df.select(item).dropDuplicates().collect()):
            for (a) in value:
                # new_columns.add([item, a])
                columns_list.append("{}:{}".format(item, a))
                print "{}:{}".format(item, a)

print columns_list

print df["City"].value_counts()


# schemaString = "customer_id name city state zip_code"
#
# schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
#
# dfCustomers = sqlContext.createDataFrame(rowRDD, schema)
# dfCustomers.s

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)])

for col in columns_list:
    struct1 = StructType().add(col, StringType(), True)
    print ("added {}".format(col))

df3 = sqlContext.createDataFrame(sc.emptyRDD(), schema)

df4 = sqlContext.createDataFrame(sc.emptyRDD(), struct1)

ag = 34
firstDF = Row(name = "Nik", age = ag)
df5 = sqlContext.createDataFrame(firstDF, schema)
# appended = df3.union(firstDF)

print firstDF
df3.collect();
df4.collect();
df5.collect();
print df3.printSchema()
print df4.printSchema()
print df5.printSchema()


# Add value to ROW
# from pyspark.sql import Row
#
# row = Row(ts=1465326926253, myid=u'1234567', mytype=u'good')
#
# # In legacy Python: Row(name=u"john", **row.asDict())
# Row(**row.asDict(), name=u"john")
# ## Row(myid='1234567', mytype='good', name='john', ts=1465326926253)



# uniq_elements = set()
#
# for row in df.collect:
#     for item in row.collect:
#
# print df.collect()



# fakeData = sqlContext.load(source="com.databricks.spark.csv", path = input_text_file, header = True,inferSchema = True)
# df.printSchema()
# print df.count()
# print len(df.columns)
# print df.columns
# df.describe().show()
# df.select('region_code').dropDuplicates().show()
# df.select('okved').dropDuplicates().show()



# import sys
# from pyspark import SparkContext, SparkConf
#
# conf = SparkConf().setAppName("csvCrowling").setMaster("local")
# sc = SparkContext(conf=conf)
#
# input_text_file=sys.argv[1]
# output_text_file=sys.argv[2]
#
# counts = sc.textFile(input_text_file).\
#        flatMap(lambda x: x.split(",")).\
#        map(lambda x: (x,1)).\
#        reduceByKey(lambda x,y: x+y)
#
# dateFromFile = sc.textFile(input_text_file)
# flatDataMap = dateFromFile.flatMap(lambda x: x.split(","))
# mapedData = flatDataMap.map(lambda x: (x,1))
# reducedData = mapedData.reduceByKey(lambda x,y: x+y)
# reducedData.saveAsTextFile(output_text_file)
