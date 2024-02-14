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

datasource = context.sources.add_spark('spark_datasource')
asset_name = 'dataframe_asset'
data_asset = datasource.add_dataframe_asset(name=asset_name)

batch_request = data_asset.build_batch_request(dataframe=dataframe)



