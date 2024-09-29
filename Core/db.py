import findspark

from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("database").getOrCreate()

df = sc.createDataFrame(
    [
        ("varad","pune"),
        ("raj","nashik"),
        ("prathamesh","a.nagar")
    ],
    ["name","city"],
)

df.show()