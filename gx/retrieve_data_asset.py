import great_expectations as gx
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = pd.DataFrame(
    {
        "a": [1, 2, 3, 4, 5, 6],
        "b": [100, 200, 300, 400, 500, 600],
        "c": ["one", "two", "three", "four", "five", "six"],
    },
    index=[10, 20, 30, 40, 50, 60],
)

dataframe = spark.createDataFrame(data=df)

context = gx.get_context()

asset = context.get_datasource('spark_datasource').get_asset('dataframe_asset')
batch_request = asset.build_batch_request(dataframe=dataframe)
batches = asset.get_batch_list_from_batch_request(batch_request)

