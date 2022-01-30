import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, isnan, when, count, col
from pyspark.sql.types import StringType

def show_total_missing_values(spark_df):
    row_count = spark_df.count()
    
    # tally the number of nulls or NaNs in a column
    nulls_df = spark_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in spark_df.columns]).toPandas()
    
    # turn this into a vertical (i.e. more legible) table
    nulls_df = pd.melt(nulls_df, var_name='column name', value_name='total missing')

    # add a column that shows the percentage of empties for each column
    nulls_df['% missing'] = 100 * nulls_df['total missing'] / row_count
    
    display(nulls_df)    

    
@udf(StringType())    
def convert_sas_date_to_datetime(sas):
    if sas:
        return (datetime(1960, 1, 1).date() + timedelta(sas)).isoformat()
    return None