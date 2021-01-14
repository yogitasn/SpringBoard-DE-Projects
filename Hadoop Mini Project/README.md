## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Setup](#setup)
* [LocalExecution](#localexecution)
* [HadoopExecution](#hadoopexecutio)

## General Info
This project is Hadoop Mini Project that uses Map reduce to get the number of accidents per make and year of the car

## Description
In this project, we need to utilize data from an automobile tracking platform that tracks the history of important incidents after the initial sale of a new vehicle. Such incidents include subsequent private sales, repairs, and accident reports. The platform provides a good reference for second-hand buyers to understand the vehicles they are interested in.

A Mapreduce program needs to be developed to get the number of accident records per make and year of the car.


## Technologies
Project is created with:
* Hortonworks HDP Sandbox 3.0.0
* Python 3.7+


## Setup

Update the below line in the python files to run locally  

```
#!<local_path_to_python.exe>

```
Update the below line in the python files to run on Hadoop

```
#!/usr/bin/env python

```

## LocalExecution

Output of Mapreduce program

* Mapper1 Output

![Alt text](screenshot/mapper1output.PNG?raw=true "Mapper1Output")

* Reducer1 Output

![Alt text](screenshot/reducer1output.PNG?raw=true "Reducer1Output")

* Mapper2 Output

![Alt text](screenshot/mapper2output.PNG?raw=true "Mapper2Output")

* Reducer2 Output

![Alt text](screenshot/reducer2output.PNG?raw=true "Reducer21Output")

## HadoopExecution

* Execution in Hadoop Distributed Environment

Move the mapreduce files to /home/hdfs and assign permissions

```
chmod +x /home/hduser/autoinc_mapper*.py

chmod +x /home/hduser/autoinc_reducer*.py

```
Create a folder in HDFS and move the data.csv file

```
hdfs dfs -mkdir /<datafolder>

```
Move the data.csv to the datafolder in HDFS

```
hdfs dfs -put <local_path>/data.csv /<datafolder>/

```
Create bashscript 'mapreduce1.sh' to execute the first set of mapreduce code

```

hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-file /home/hdfs/autoinc_mapper1.py    -mapper /home/hdfs/autoinc_mapper1.py \
-file /home/hdfs/autoinc_reducer1.py   -reducer /home/hdfs/autoinc_reducer1.py \
-input /<datafolder>/data.csv \
-output /output

```

```
sh mapreduce.sh

```

After execution, the output will be saved to the 'output folder'

```

[hdfs@sandbox-hdp ~]$ hdfs dfs -cat /output/part-00000                                                                                                                                                            
EXOA00341AB123456       Mercedes      2016
INU45KIOOPA343980       Mercedes        2015                
UXIA769ABCC447906       Toyota  2017                               
VOME254OOXW344325       Mercedes        2015                      
VXIO456XLBB630221       Nissan  2003

```

Repeat the same steps for mapreduce1.sh

The output should be as follows

```

[hdfs@sandbox-hdp ~]$ hdfs dfs -cat /accidents_count_per_make_year/part-00000                                                                                                                                           
Toyota  2017    1                                                              
Mercedes        2016    1                                                    
Mercedes        2015  2                               
Nissan  2003    1       

```

