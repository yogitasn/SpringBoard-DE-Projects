Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Optimization](#optimization)

## General Info
This project is Spark Optimization Mini Project that will optimize existing Pyspark code to improve its performance.

## Description
Spark can give you a tremendous advantage when it comes quickly processing massive datasets. However, the tool is only as powerful as the one who wields it. Spark performance can become sluggish if poor decisions are made in the layout of the code and the functions
that are chosen.


## Technologies
Project is created with:
* Python 3.7+
* Spark2

## Optimization

Following modifications were done to optimize the code

```
The default no of partitions in Spark is 200. 

questionsDF.rdd.getNumPartitions()=4

answers_month.rdd.getNumPartitions()=200

To avoid more shuffles in the join step,  reduced the no. of partitions using coalesce and set the same no. of partitions for both dataframes in the join condition 

answers_month=answers_month.coalesce(4)

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'title', 'month', 'cnt')

Also, removed the redundant column 'creation_date' in the select step and noticed an improvement in performance

```

* Physical Plan No Optimization

![Alt text](Screenshot/NoOptimizationExplainOutput.PNG?raw=true "PhysicalPlanNoOptimization")

* Spark Job Execution Time No Optimization

![Alt text](Screenshot/SparkJobNoOptimization.PNG?raw=true "SparkJobNoOptimization")

* Physical Plan After Optimization

![Alt text](Screenshot/AfterOptimizationExplainOutput.PNG?raw=true "PhysicalPlanAfterOptimization")

* Spark Job Execution Time After Optimization

![Alt text](Screenshot/SparkJobwithOptimization.PNG?raw=true "SparkJobwithOptimization")
