Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [HadoopSetupandExecution](#hadoopsetupandexecution)

## General Info
This project is Spark Mini Project that leverages spark to get the number of accidents per make and year of the car

## Description
In this project, we need to utilize data from an automobile tracking platform that tracks the history of important incidents after the initial sale of a new vehicle. Such incidents include subsequent private sales, repairs, and accident reports. The platform provides a good reference for second-hand buyers to understand the vehicles they are interested in.

A Spark program needs to be developed to get the number of accident records per make and year of the car.


## Technologies
Project is created with:
* Hortonworks HDP Sandbox 3.0.0
* Python 3.7+
* Spark2


## HadoopSetupandExecution

* Execution in Hadoop Distributed Environment

Move the python file with spark code to /home/hdfs and assign permissions

```
chmod +x /home/hduser/autoinc_spark.py

```
Create a folder in HDFS and move the data.csv file

```
hdfs dfs -mkdir /<datafolder>

```
*Note: Change the datafolder name in the python file
```
raw_rdd = spark.sparkContext.\
           textFile("/<datafolder>/data.csv")
```
Move the 'data.csv' to the datafolder in HDFS

```
hdfs dfs -put <local_path>/data.csv /<datafolder>/

```
Create bashscript 'spark-job.sh' to execute the spark code

```

/bin/spark-submit autoinc_spark.py

```

```
sh spark-job.sh

```

After execution, the output will be saved to the 'accident_records_count folder'

```

[hdfs@sandbox-hdp ~]$ hdfs dfs -ls accident_records_count/                                                                                                                                                              
Found 3 items                                                                                                                                                                                                           
-rw-r--r--   1 hdfs hdfs          0 2021-02-09 04:39 accident_records_count/_SUCCESS
-rw-r--r--   1 hdfs hdfs         30 2021-02-09 04:39 accident_records_count/part-00000
-rw-r--r--   1 hdfs hdfs         30 2021-02-09 04:39 accident_records_count/part-00001                                                  

```

```

[hdfs@sandbox-hdp ~]$ hdfs dfs -cat accident_records_count/part-00000                     
Toyota-2017,1
Mercedes-2015,2                                                     
                                                                                                 
[hdfs@sandbox-hdp ~]$ hdfs dfs -cat accident_records_count/part-00001                                                                                                                                                   
Mercedes-2016,1                                                               
Nissan-2003,1 

```


* Spark job run output log

![Alt text](screenshot/spark_job_output.PNG?raw=true "SparkJobLog")

* Final Text file output

![Alt text](screenshot/outputfile.PNG?raw=true "Outputfile")

* File 'sparkjobrunlog.txt' is the command line execution log of successful spark job run