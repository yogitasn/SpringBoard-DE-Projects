import findspark

findspark.init()

findspark.find()

import pyspark

findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

    
from typing import List
import datetime
import decimal

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3]) 

answers_input_path = os.path.join(project_path, 'C:/Optimization/data/answers')

questions_input_path = os.path.join(project_path, 'C:/Optimization/data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

'''
Answers aggregation

Here we : get number of answers per question per month
'''

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

# questionsDF has 4 partitions and answers_month has 200 so reducing the no of partitions using coalesce to reduce the no of shuffles in the below join transformation
answers_month=answers_month.coalesce(4)

# Removed the redundant column 'creation_date'
resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show(truncate=False)

resultDF.explain()