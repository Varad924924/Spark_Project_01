import findspark

findspark.init()

from pyspark.sql import SparkSession

sc = SparkSession.builder.appName('FileReading').getOrCreate()

data = sc.sparkContext.parallelize({1,2,3,4,5,6,7,8,9,10})

data1 = sc.sparkContext.parallelize({1,3,5,7,12,23,45})

data2 = data.union(data1)
data3 = data.intersection((data1))

print(f"union is {data2.collect()}")
print(f"intersection is {data3.collect()}")
sumdata = data.reduce(lambda i,j:(i+j))

print(f"total sum of list {sumdata}")