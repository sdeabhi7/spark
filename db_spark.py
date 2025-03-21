from pyspark.sql import SparkSession
import pandas as pd
 
spark = SparkSession.builder.appName("ExportTableSchemas").getOrCreate()

# To view all databases
databases = spark.catalog.listDatabases()
for db in databases:
    print(db.name)

# To view all table of database
db = 'prod'
spark.catalog.setCurrentDatabase(db)
tables = spark.catalog.listTables()

for table in tables:
    print(table.name)

# To print schema of table
table = 'prod.prod_data'
store = spark.table(table)
store.printSchema()

# read data from the table
s = spark.read.table("prod.prod_data")

spark.table('prod.prod_data')

spark.sql("show tables in prod").show()

spark.sql('select product, count(*) from prod.prod_data group by product').show()