#!/usr/bin/env python
# coding: utf-8

# In[1]:

# importing all libraries
import pandas as pd
from pyspark.sql import SparkSession
import functools
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import time
import os
from pyspark.sql.functions import col


# In[2]:


import os
print(os.listdir(
    'F:\\IITJ\\Semester_II\\SDE\\SDE Assignment 3\\Dataset\\Stock_market_dataset'))


# In[ ]:


file_list = ['ADANIPORTS_with_indicators_.csv',
             'ASIANPAINT_with_indicators_.csv',
             'AXISBANK_with_indicators_.csv']

# In[3]:

'''
file_list = ['ADANIPORTS_with_indicators_.csv',
             'ASIANPAINT_with_indicators_.csv',
             'AXISBANK_with_indicators_.csv',
             'BAJAJFINSV_with_indicators_.csv',
             'BAJFINANCE_with_indicators_.csv']
'''
print(file_list)

# file_list=['ADANIPORTS_with_indicators_.csv',
#  'ASIANPAINT_with_indicators_.csv',
#  'AXISBANK_with_indicators_.csv',
#  'BAJAJFINSV_with_indicators_.csv',
#  'BAJFINANCE_with_indicators_.csv',
#  'BHARTIARTL_with_indicators_.csv',
#  'BPCL_with_indicators_.csv',
#  'BRITANNIA_with_indicators_.csv',
#  'CIPLA_with_indicators_.csv',
#  'COALINDIA_with_indicators_.csv']

# In[4]:


# file_list


# In[5]:


# pwd


# In[6]:


# os.listdir()


# **BENCHMARKING QUERIES: \
# Query 1: SELECT SUM(open) from data; \
# Query 2: SELECT MAX(high) from data; \
# Query 3: SELECT SUM(volume) from data;**

# In[7]:


# Benchmark queries


# In[8]:

# Benchmark queries
# explicit function
def spark_unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


def spark_query1(dfs):
    return int(dfs.agg({"open": "sum"}).collect()[0][0])


def spark_query2(dfs):
    return (dfs.agg({"high": "max"}).collect()[0][0])


def spark_query3(dfs):
    return int(dfs.agg({"volume": "sum"}).collect()[0][0])


# In[9]:


def spark_benchmark(file_list):
    spark_st = time.time()
    spark = SparkSession.builder.appName(
        'Read All CSV Files in Directory').getOrCreate()
    spark_dfs = []
    spark_file_size = 0
    print(spark_file_size)
    with open("spark_timing_log_3_files.txt", "w") as fp:
        for ind, s_file in enumerate(file_list):
            print(s_file)
            df = spark.read.option('delimiter', ',').csv(s_file, header=True)

            file_size = os.path.getsize(s_file)
            print(file_size)
            print(file_list)
            spark_file_size += file_size
            spark_dfs.append(df)
            fp.write("X----X-----X-----X----X")
            fp.write("\n")
            fp.write("file name is {} and total file size becomes {} MB".format(str(s_file),
                                                                                str(spark_file_size / (1024*1024))))

            #print(str(round((spark_file_size / (1024*1024)),2)))
            fp.write("\n")
            temp_df = spark_unionAll(spark_dfs)
            temp_et = time.time()

            # get the execution time
            t_elapsed_time = temp_et - spark_st
            # fp.write("\n")
            fp.write("Time takes to merge : {} seconds.".format(
                str(t_elapsed_time)))
            fp.write("\n")
            q1_st = time.time()

            # fp.write("X----X-----X-----X----X")
            total_open = spark_query1(temp_df)
            q1_et = time.time()
            # get the execution time
            q1_elapsed_time = q1_et - q1_st
            total_q1_time = q1_et - spark_st
            fp.write("To Run Query sum of opening price of stocks {} INR it takes {} seconds and Total Time Taken {} seconds.".format(
                str(total_open), str(q1_elapsed_time), str(total_q1_time)))
            fp.write("\n")
            # fp.write("X----X-----X-----X----X")
            q2_st = time.time()
            max_high_price = spark_query2(temp_df)
            q2_et = time.time()
            q2_elapsed_time = q2_et - q2_st
            total_q2_time = q2_et - spark_st
            fp.write("To Run Query to get maximun high price of stocks {} INR it takes {} seconds and Total Time Taken {} seconds.".format(str(max_high_price),
                                                                                                                                           str(
                q2_elapsed_time),
                str(total_q2_time)))
            fp.write("\n")
            # fp.write("X----X-----X-----X----X")
            q3_st = time.time()
            total_volume = spark_query3(temp_df)
            q3_et = time.time()
            q3_elapsed_time = q3_et - q3_st
            total_q3_time = q3_et - spark_st
            fp.write("To Run Query total volume of stocks {} INR it takes {} seconds and Total Time Taken {} seconds.".format(str(total_volume),
                                                                                                                              str(
                q3_elapsed_time),
                str(total_q3_time)))
            fp.write("\n")
            # fp.write("X----X-----X-----X----X")
        # spark_union_df = spark_unionAll(spark_dfs)

        # print(spark_union_df.show())
        spark_et = time.time()

        # get the execution time
        elapsed_time = spark_et - spark_st
        elapsed_time = round(elapsed_time, 2)
        fp.write('Spark Execution time in total is: {} seconds'.format(
            str(elapsed_time)))
        print(elapsed_time)
        fp.write("\n")


# In[10]:


# execute spark benchmark function
spark_benchmark(file_list)


# In[ ]:


# In[ ]:


# In[ ]:


# In[11]:


# Benchmark queries for pandas

def pd_query1(dfs):
    return dfs.open.sum()


def pd_query2(dfs):
    return dfs.high.max()


def pd_query3(dfs):
    return dfs.volume.sum()


# In[12]:

# defining pandas bechmark function
def pandas_benchmark(file_list):
    pd_st = time.time()
    pd_dfs = []
    pd_file_size = 0
    print(pd_file_size)
    with open("pandas_timing_log_3_files.txt", "w") as fd:
        for s_file in file_list:
            print(s_file)
            df = pd.read_csv(s_file)
            pd_dfs.append(df)
            file_size = os.path.getsize(s_file)
            print(file_size)
            pd_file_size += file_size
            print(pd_file_size)
            fd.write("X----X-----X-----X----X")
            fd.write("\n")
            fd.write("file name is {} and total file size becomes {} MB".format(str(s_file),
                                                                                str(pd_file_size / (1024*1024))))
            fd.write("\n")
            temp_df = pd.concat(pd_dfs)
            fd.write("total rows in dataframe is : {}".format(str(len(temp_df))))
            fd.write("\n")
            # print(pd_union_df.head())

            temp_et = time.time()

            # get the execution time
            # fd.write("X----X-----X-----X----X")
            t_elapsed_time = temp_et - pd_st
            fd.write("Time takes to merge is {} seconds".format(
                str(t_elapsed_time)))
            fd.write("\n")
            q1_st = time.time()
            total_open = pd_query1(temp_df)
            q1_et = time.time()
            # get the execution time
            q1_elapsed_time = q1_et - q1_st
            total_q1_time = q1_et - pd_st
            fd.write("To Run Query to get total sum of opening price of stocks : {} INR,  it takes {} seconds and Total Time Taken is {} seconds".format(str(total_open),
                                                                                                                                                         str(
                q1_elapsed_time),
                str(total_q1_time)))
            fd.write("\n")
            # fd.write("X----X-----X-----X----X")
            q2_st = time.time()
            max_high = pd_query2(temp_df)
            q2_et = time.time()
            q2_elapsed_time = q2_et - q2_st
            total_q2_time = q2_et - pd_st
            fd.write("To Run Query to get maximun high price of stocks : {} INR, it takes {} seconds and Total Time Taken is {} seconds".format(str(max_high),
                                                                                                                                                str(
                q2_elapsed_time),
                str(total_q2_time)))
            fd.write("\n")
            # fd.write("X----X-----X-----X----X")
            q3_st = time.time()
            total_volume = pd_query3(temp_df)
            q3_et = time.time()
            q3_elapsed_time = q3_et - q3_st
            total_q3_time = q3_et - pd_st
            fd.write("To Run Query to get total volume of stocks: {}, it takes {} seconds and Total Time Taken is {} seconds".format(str(total_volume),
                                                                                                                                     str(
                q3_elapsed_time),
                str(total_q3_time)))
            fd.write("\n")
            # fd.write("X----X-----X-----X----X")
        pd_et = time.time()

        # get the execution time
        elapsed_time = pd_et - pd_st
        elapsed_time = round(elapsed_time, 2)
        fd.write('Pandas Execution time in total is: {} seconds'.format(
            str(elapsed_time)))
        print(elapsed_time)
        fd.write("\n")


# In[13]:


# execute pandas benchmark queries
pandas_benchmark(file_list)


# In[ ]:
