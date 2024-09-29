import findspark
from numpy.ma.core import inner

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

sc = SparkSession.builder.appName("FileReadProg").getOrCreate()
sqlCon = SQLContext(sc)

df = sqlCon.read.csv("C:/Users/admin/PycharmProjects/SparkProject/userinfo.csv" , header=True , inferSchema=True)

df1 = sqlCon.read.csv("C:/Users/admin/PycharmProjects/SparkProject/UserFinDetail.csv" , header=True , inferSchema=True)

print(df.show())
print(df1.show())

df.createOrReplaceTempView("userinfo")
df1.createOrReplaceTempView("UserFinDetail")

innerJoin = sqlCon.sql("select A.City,A.Company, A.Age,  B.* from userinfo A JOIN UserFinDetail B ON A.Mob = B.Mob")
print("Dispalying comman data from user")

print(innerJoin.show())

LeftJoin = sqlCon.sql("select A.City,A.Company, A.Age,  B.* from userinfo A LEFT JOIN UserFinDetail B ON A.Mob = B.Mob")
print("Dispalying comman data from user")

print(innerJoin.show())

RightJoin = sqlCon.sql("select A.* ,  B.CIBIL , B.NAME from userinfo A RIGHT JOIN UserFinDetail B ON A.Mob = B.Mob")
print("Dispalying comman data from user")

print(innerJoin.show())