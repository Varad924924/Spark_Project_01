import findspark
findspark.init()

from pyspark.sql import SparkSession

sc = SparkSession.builder.appName('FileReading').getOrCreate()

file = sc.sparkContext.textFile('D:\\prwatech\\Ganesha.txt')
print(file.getNumPartitions())
div = file.flatMap(lambda x: x.split(" "))
keybyvalue = div.map(lambda a:(a,1))
agg = keybyvalue.reduceByKey(lambda x,y :(x+y))
print(file.collect())
print(div.collect())
print(keybyvalue.collect())
print(agg.collect())