from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.types import * 
from pyspark.sql.functions import lit
import pyodbc
import pandas as pd
import pyspark.sql.functions as sf
from datetime import date, timedelta
import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import col
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from sqlalchemy import create_engine

spark = SparkSession.builder.config("spark.driver.memory", "15g").getOrCreate()

def generate_time_range_data(startdate,enddate):
    format = '%Y-%m-%d'
    day_time = []
    startdate = datetime.datetime.strptime(startdate, format).date()
    enddate = datetime.datetime.strptime(enddate, format).date()
    delta = enddate - startdate
    for i in range(delta.days + 1):
        day = startdate + timedelta(days=i)
        day = day.strftime('%Y%m%d')
        day_time.append(str(day))
    return day_time

def process_logsearch_t6():
    day_range = generate_time_range_data('2022-06-01','2022-06-14')
    path = "C:\\Users\\ASUS\\OneDrive\\Big_Data_Analytics\\Dataset\\log_search\\"
    df = spark.read.parquet('C:\\Users\\ASUS\\OneDrive\\Big_Data_Analytics\\Dataset\\log_search\\20220601')
    df = df.filter(df.user_id.isNotNull())
    df = df.select('datetime','user_id','keyword')
    result_t6 = df
    for i in day_range[1:]:
        df1 = spark.read.parquet(path+i)
        df1 = df1.filter(df1.user_id.isNotNull())
        df1 = df1.select('datetime','user_id','keyword')
        result_t6 = result_t6.union(df1)
    result_t6 = result_t6.drop(result_t6.datetime)
    result_t6 = result_t6.filter(result_t6.keyword.isNotNull())    
    return result_t6

def process_logsearch_t7():
    day_range = generate_time_range_data('2022-07-01','2022-07-14')
    path = "C:\\Users\\ASUS\\OneDrive\\Big_Data_Analytics\\Dataset\\log_search\\"
    df = spark.read.parquet('C:\\Users\\ASUS\\OneDrive\\Big_Data_Analytics\\Dataset\\log_search\\20220701')
    df = df.filter(df.user_id.isNotNull())
    df = df.select('datetime','user_id','keyword')
    result_t7 = df
    for i in day_range[1:]:
        df1 = spark.read.parquet(path+i)
        df1 = df1.filter(df1.user_id.isNotNull())
        df1 = df1.select('datetime','user_id','keyword')
        result_t7 = result_t7.union(df1)
    result_t7 = result_t7.drop(result_t7.datetime)
    result_t7 = result_t7.filter(result_t7.keyword.isNotNull())
    return result_t7

def ranking_data(process_result):
    window = Window.partitionBy("user_id").orderBy(col('keyword').desc())
    rank_result = process_result.withColumn('RANK',rank().over(window))
    rank_result = rank_result.filter(rank_result.RANK == '1')
    rank_result = rank_result.distinct()
    return rank_result

def process_most_search(result_t7,result_t6):
    df = spark.read.csv('C:\\Users\\ASUS\\OneDrive\\Big_Data_Analytics\\BigData_Gen2\\Class 6\\HabbitResult',header=True)
    df= df.withColumn('index', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
    result_t7 = result_t7.withColumn('index', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
    result_t6 = result_t6.withColumn('index', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
    df_t7 = df.join(result_t7, df.index == result_t7.index,'inner').drop(result_t7.user_id).drop(result_t7.index).withColumnRenamed('keyword','T7_Keyword')
    df_t6 = df.join(result_t6, df.index == result_t6.index,'inner').drop(result_t6.user_id).drop(result_t6.index).withColumnRenamed('keyword','T6_Keyword')
    result = df_t6.join(df_t7,df_t6.index == df_t7.index,'inner').select(df_t7.Contract,df_t7.Date,df_t7.SportDuration,df_t7.TVDuration,df_t7.ChildDuration,df_t7.RelaxDuration,df_t7.MovieDuration,df_t7.Most_Watch,df_t7.T7_Keyword,df_t6.T6_Keyword)
    return result

def import_to_mysql(result):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'Data_Engineer'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = ''
    result = result.withColumnRenamed('Most_Watch','MostWatch')
    result.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','customer_behaviour').option('user',user).option('password',password).mode('append').save()
    return print("Data Import Successfully")

def main():
    print("----------------------------")
    print("Starting process log search t6 ")
    print("----------------------------")
    result_t6 = process_logsearch_t6()
    print("----------------------------")
    print("Starting process log search t7")
    print("----------------------------")
    result_t7 = process_logsearch_t7()
    print("----------------------------")
    print("processed log search summary t6")
    print("----------------------------")
    result_t6.groupBy('keyword').count().orderBy(col('count').desc()).show(truncate=False)
    print("----------------------------")
    print("processed log search summary t7")
    print("----------------------------")
    result_t7.groupBy('keyword').count().orderBy(col('count').desc()).show(truncate=False)
    print("----------------------------")
    print("processed ranking data t6")
    print("----------------------------")
    rank_result_t6 = ranking_data(result_t6)
    print("----------------------------")
    print("processed ranking data t7")
    print("----------------------------")
    rank_result_t7 = ranking_data(result_t7)
    print("----------------------------")
    print("finalizing ranking result")
    print("----------------------------")
    result = process_most_search(result_t7,result_t6)
    result.show()
    print("----------------------------")
    print("Importing result to Server")
    print("----------------------------")
    import_to_mysql(result)
    return print("Job finished")

try :
    main()
except :
    print("Code Error")