from re import split

#import findspark

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

sc = SparkSession.builder.appName("FileReadProg").getOrCreate()
sqlCon = SQLContext(sc)

file = sqlCon.read.csv("C:/Users/admin/PycharmProjects/SparkProject/Chennai01.csv" , header=True , inferSchema=True)

print("This is total count",file.count())

file.registerTempTable("ChennaiRealEstate")

query2 = sqlCon.sql("select Location, count(Location) as LocationCount "
                    "from ChennaiRealEstate "
                    "group by Location having LocationCount>100 order "
                    "by LocationCount DESC")

query3 = sqlCon.sql("select * from ChennaiRealEstate "
                    "where Area > 2000 and Price < 15000000 "
                    "and Location IN ('Madhavaram' , 'Karapakkam')")

query4 = sqlCon.sql("select * from ChennaiRealEstate "
                    "order by Price DESC "
                    "limit 1 ")

query5 = sqlCon.sql("select avg(price) as ap "
                    "from ChennaiRealEstate "
                    "where BHK = 3 ")

print(query2.show())
print(query3.show())
print(query4.show())
print(query5.show())