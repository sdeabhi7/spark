import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, concat

start = time.time()

spark = SparkSession.builder.appName("fetchdata").getOrCreate()

all_data = None

for i in ['2024', '2025']:
    data = spark.read.table(f'patch_{i}.firmware')
    
    renamed_data = data.select(
        'country',
        'industry',
        'patch_id',
        data['version_id'].alias('version'),
        data['patch_date'].alias('date'),
        split(data['patch_date'], ' ').getItem(0).alias('year'),
        concat(split('industry', ' ').getItem(0), data['version_id']).alias('industry_patch')
    )
    
    distinct_data = renamed_data.distinct()
    
    if all_data is None:
        all_data = distinct_data
    else:
        all_data = all_data.union(distinct_data)

all_data = all_data.coalesce(1)
       
output_path = "sdeabhi/firmware/patch.csv"  
all_data.write.mode("overwrite").csv(output_path, header=True)
print(f"Data saved to {output_path}")

end = time.time()
print(f'Time Taken {end - start}')