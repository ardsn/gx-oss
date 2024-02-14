import great_expectations as gx
import pandas as pd
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

# Creates a Spark dataframe
df = pd.DataFrame(
    {
        'a': [1, 2, 3, 4, 5, 6],
        'b': [100, 200, 300, 400, 500, 600],
        'c': ['one', 'two', 'three', 'four', 'five', 'six'],
    },
    index=[10, 20, 30, 40, 50, 60],
)
dataframe = spark.createDataFrame(data=df)

# Retrieves an existing datasource and data asset
context = gx.get_context()
datasource = context.get_datasource('spark_datasource')
asset_name = 'dataframe_asset'
data_asset = datasource.get_asset(asset_name)
# Adds content to the data asset
batch_request = data_asset.build_batch_request(dataframe=dataframe)

# Creates a validator
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name='my_suite',
)
validator.head()

# Runs an expectation
validation_result = validator.expect_column_values_to_be_of_type(
    column='c',
    type_='StringType', 
    mostly=0.9,
    result_format='COMPLETE',
    include_config=False,
    catch_exceptions=False,
    meta={},
)
print(validation_result)

# Checks if the validation result failed
if not validation_result['success']:
    raise Exception('Validation failed!')

# Persists the expectation suite and its edited expectations
validator.save_expectation_suite(discard_failed_expectations=False)
