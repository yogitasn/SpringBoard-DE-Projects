import os, sys, subprocess
from datetime import datetime
import pandas as pd
from pandas import Series, DataFrame


def global_SQLContext(spark1):
    global spark
    spark = spark1


def rename_log_filename(logfile):
    os.rename(log_filename, logfile)
    log_filename = logfile

def initial_log_file(logfile):
    global log_filename, g_main_script
    #Get current system timestamp
    log_filename = logfile


    current_time = datetime.now()
    str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
    time_index = current_time.strftime('%d%H%M%S%f')
    
    message_list =[]

    # Define log file name
    parse = sys.argv[0].split('/')
    g_main_script = parse[-1]

    log_print('Log filename: {}'.format(log_filename))

    message_list.append('============================================================================================')
    message_list.append('===== SCRIPT: {} started at {} ===='.format(g_main_script,str_current_time))
    message_list.append('============================================================================================')
    message_dict={}
    message_dict={'Message':message_list, 'TimeIndex': time_index}

    message_dict={}

    message_dict={'Message': message_list,'TimeIndex': time_index}
    df= pd.DataFrame(message_dict, columns=['Message','TimeIndex'])
    df= spark.createDataFrame(df)
    df.coalesce(1).write.mode("overwrite").format("text").partitionBy('TimeIndex').save(log_filename)

    
def complete_log_file():
    current_time = datetime.now()
    str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
    time_index = current_time.strftime('%d%H%M%S%f')

    message_list =[]

    message_list.append('============================================================================================')
    message_list.append('===== SCRIPT: {} completed at {} ===='.format(g_main_script,str_current_time))
    message_list.append('============================================================================================')
    message_dict={}
    message_dict={'Message':message_list, 'TimeIndex': time_index}

    message_dict={}

    message_dict={'Message': message_list,'TimeIndex': time_index}
    df= pd.DataFrame(message_dict, columns=['Message','TimeIndex'])
    df= spark.createDataFrame(df)

    df.coalesce(1).write.mode("append").format("text").partitionBy('TimeIndex').save(log_filename)

def log_print(log_message="Print Message"):
    current_time = datetime.now()
    str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
    time_index = current_time.strftime('%d%H%M%S%f')

    message_list =[]

    display_message=str(log_message)
    message_list.append(display_message)
    message_list.append(' ')

    message_dict={}
    message_dict={'Message':message_list, 'TimeIndex': time_index}

    df= pd.DataFrame(message_dict, columns=['Message','TimeIndex'])
    df= spark.createDataFrame(df)

    df.coalesce(1).write.mode("append").format("text").partitionBy('TimeIndex').save(log_filename)


def log_info(caller_script, log_message="Process Completed Successfully"):
    current_time = datetime.now()
    str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
    time_index = current_time.strftime('%d%H%M%S%f')

    message_list =[]
    message_list.append('===== SCRIPT: {} completed at {} ===='.format(g_main_script,str_current_time))
   
    display_message = "{}".format(log_message)

    message_list.append(display_message)
    message_list.append(' ')

    message_dict={}
    message_dict={'Message':message_list, 'TimeIndex': time_index}

    df= pd.DataFrame(message_dict, columns=['Message','TimeIndex'])
    df= spark.createDataFrame(df)

    df.coalesce(1).write.mode("append").format("text").partitionBy('TimeIndex').save(log_filename)  


def log_error(caller_script, log_message="Script Completed Successfully", return_code=123):
    current_time = datetime.now()
    str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
    time_index = current_time.strftime('%d%H%M%S%f')

    message_list =[]
    
    if return_code == 123:
        rc_message = ''
    else:
        rc_message = ' -RC: ' +str(return_code)
        border='!!!'

    top_message = caller_script + ': ' +str_current_time
    str_message = "{} {}".format(log_message, rc_message)
    #log_length = min(len(top_message), len(str_message))
    log_length = len(str_message)

    if log_length > 120:
        log_length = 120

    log_message= '=========='

    for i in range(0, log_length):
        log_message +='='
    
    border = '!!!'

    message_list.append("{}".format(log_message))
    log_message = border+ ' ' +str_message

    message_list.append("{}".format(log_message))
 #   log_message = border+ ' ' +top_message

    #message_list.append("{}".format(log_message))
    log_message ='=========='

    for i in range(0, log_length):
        log_message +='='
    
    message_list.append("{}".format(log_message))
    message_list.append(' ')
    
    message_dict={}
    message_dict={'Message':message_list, 'TimeIndex': time_index}

    df= pd.DataFrame(message_dict, columns=['Message','TimeIndex'])
    df= spark.createDataFrame(df)

    df.coalesce(1).write.mode("append").format("text").partitionBy('TimeIndex').save(log_filename)  

    return return_code


def log_step(caller_script, log_message ="Script Completed Successfully"):
    current_time = datetime.now()
    str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
    time_index = current_time.strftime('%d%H%M%S%f')

    message_list =[]
    
    str_message =caller_script+' '+log_message
    if len(str_message) > 120:
        log_length = 120
    else:
        log_length = len(str_message)
     
    log_message = '==='

    for i in range(0,log_length):
        log_message += '='

    log_message +='==='

    message_list.append("{}".format(log_message))
    log_message='** '+str_message+ ' **'

    message_list.append("{}".format(log_message))
    log_message = '===='

    for i in range(0,log_length):
        log_message +='='

    log_message+='==='
    message_list.append("{}".format(log_message))
    log_message = 'Started: {}'.format(str_current_time)
    message_list.append("{}".format(log_message))
    message_list.append(' ')
    message_dict={}

    message_dict={}
    message_dict={'Message':message_list, 'TimeIndex': time_index}

    df= pd.DataFrame(message_dict, columns=['Message','TimeIndex'])
    df= spark.createDataFrame(df)

    df.coalesce(1).write.mode("append").format("text").partitionBy('TimeIndex').save(log_filename)  


if __name__=='__main__':
    log_error(sys.argv[0],sys.argv[1],sys.argv[2])