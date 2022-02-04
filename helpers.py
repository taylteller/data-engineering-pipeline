import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, isnan, when, count, col
from pyspark.sql.types import StringType


def show_total_missing_values(spark_df):
    """
    Display the number and percentage of null or invalid values per dataframe field
    """
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
    """
    Accepts a SAS date and returns it as a datetime
    """
    if sas:
        return (datetime(1960, 1, 1).date() + timedelta(sas)).isoformat()
    return None


def tables_exist(df_list):
    """
    Take a list of dataframes and ensure they exist
    """
    all_exist = True
    for i, df in enumerate(df_list):
        if df is None:
            all_exist = False
            print(f'Dataframe at index {i} does not exist')
    if all_exist == True:
        print('All tables exist')
        

def tables_contain_data(df_list):
    """
    Take a list of dataframes and ensure they all contain content.
    Also print the number of rows for each dataframe
    """
    all_contain = True
    for i, df in enumerate(df_list):
        count = df.count()
        if count == 0:
            all_contain = False
            print(f'Dataframe at index {i} does not contain any rows')
        else:
            print(f'Dataframe at index {i} contains {count} rows')
    if all_contain == True:
        print('All tables contain content')