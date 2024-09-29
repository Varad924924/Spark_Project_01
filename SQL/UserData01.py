import findspark

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


sc = SparkSession.builder.appName("FileReadProg").getOrCreate()
sqlCon = SQLContext(sc)

df = sqlCon.read.csv("C:/Users/admin/PycharmProjects/SparkProject/userinfo.csv" , header=True , inferSchema=True)

df1 = sqlCon.read.csv("C:/Users/admin/PycharmProjects/SparkProject/UserFinDetail.csv" , header=True , inferSchema=True)

print(df.show())
print(df1.show())

inner_join = df.join(df1 , df['Mob'] == df1['Mob'], "inner")\
             .select(df.Age , df.Company, df1.CIBIL, df1.Mob) \
             .where(df1.CIBIL > 700)
print("inner_join")
print(inner_join.show())

cross_join = df.join(df1 , df['Mob'] == df1['Mob'], "cross")\
             .select(df.Age , df.Company, df1.CIBIL, df1.Mob)

print("Cross_join")
print(cross_join.show())

outer_join = df.join(df1 , df['Mob'] == df1['Mob'], "outer")\
             .select(df.Age , df.Company, df1.CIBIL, df1.Mob)

print("outer_join")
print(outer_join.show())

left_join = df.join(df1 , df['Mob'] == df1['Mob'], "left")\
             .select(df.Age , df.Company, df1.CIBIL, df1.Mob)

print("left_join")
print(left_join.show())

right_join = df.join(df1 , df['Mob'] == df1['Mob'], "right")\
             .select(df.Age , df.Company, df1.CIBIL, df1.Mob)

print("right_join")
print(right_join.show())

leftSemi_join = df.join(df1 , df['Mob'] == df1['Mob'], "leftSemi")\
             .select(df.Age , df.Company )

print("leftSemi_join")
print(leftSemi_join.show())

leftAnti_join = df.join(df1 , df['Mob'] == df1['Mob'], "leftAnti")\
             .select(df.Age , df.Company)

print("leftAnti_join")
print(leftAnti_join.show())