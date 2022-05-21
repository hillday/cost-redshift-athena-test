#!/usr/bin/python3

import threading
import time
import sys
import getopt
import random
import os
import csv
import psycopg2
import boto3
import json

argv = sys.argv[1:]
if len(argv) == 0:
    print("Error,the thread count is must!.use example: python cost_test.py -tn 10")
    exit(1)
 
g_thread_num  = int(sys.argv[1])
g_sql_num = 35
g_sql_collections = []
g_log_path = ""
g_program_start = round(time.time())
g_program_runtime =1 * 60 * 60
g_athena_client = None

def init_sql_collections():
    sql_path = os.path.join(os.getcwd(), "sql")
    if os.path.exists(sql_path):
        for sql_id in range(g_sql_num):
            sql_file = os.path.join(sql_path,str(sql_id) + ".sql")
            file = open(sql_file)
            sql_content = file.read()
            g_sql_collections.append(sql_content)
            file.close()
    else:
        print("Error,can not found sql path")
        exit(1)

class workThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        print ("开始线程：" + self.name)
        run_sql(self.name)
        print ("退出线程：" + self.name)

class costTestThread(threading.Thread):
    def __init__(self, threadID, name,sql):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.sql = sql
    def run(self):
        if self.name == "redshift":
            run_to_redshift(self.sql)
        
        if self.name == "athena":
            run_to_athena(self.sql)

def run_sql(threadName):
    while True:
        now_time = round(time.time())
        if now_time - g_program_start > g_program_runtime:
            break
        rsql = get_execute_sql_id()
        thread_redshift = costTestThread(1, "redshift", rsql)
        thread_athena = costTestThread(2, "athena", rsql)
        thread_redshift.start()
        thread_athena.start()
        thread_athena.join()
        thread_redshift.join()
        try:
            # sleep thead 5s
            time.sleep(5) 
        except:
            print("Error,sleep")

def get_execute_sql_id():
    return random.randrange(len(g_sql_collections))


def connectPostgreSQL():
    conn = psycopg2.connect(database="xx", user="xx", password="xx",
    host="xxx", port="5439")
    print('connect successful!')
    return conn

def run_to_redshift(sqlid):
    sql = g_sql_collections[sqlid]
    conn = connectPostgreSQL()
    ##ms
    start_time = round(time.time()*1000)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        cursor.fetchone()
    except:
        print("error execute redshift sql " + str(sqlid))
    end_time = round(time.time()*1000)
    conn.commit()
    conn.close()
    write_log("redshift",sqlid,end_time - start_time,0)


def run_to_athena(sqlid):
    sql = g_sql_collections[sqlid]
    try:
        response = g_athena_client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={
            
                'Database': 'tpcds_orc_1000'
            },
            ResultConfiguration={
            
                'OutputLocation': 's3://xxxx/xxx/',
                'EncryptionConfiguration': {
            
                    'EncryptionOption': 'SSE_S3'
                }
            }
        )
        statistics = has_query_succeeded(response["QueryExecutionId"])
        write_log("athena",sqlid,statistics["TotalExecutionTimeInMillis"],statistics["DataScannedInBytes"])
    except Exception as err:
        write_log("athena",sqlid,0,-1)
        print(err)
    

def has_query_succeeded(execution_id):
    state = "RUNNING"
    max_execution = 100 * 3

    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
        max_execution -= 1
        response = g_athena_client.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in response
            and "Status" in response["QueryExecution"]
            and "State" in response["QueryExecution"]["Status"]
        ):
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return response["QueryExecution"]["Statistics"]

        time.sleep(3)

    return False


def write_log(engine,sqlid,costtime,scandata):
    print(engine+" " + str(sqlid) + " " + str(costtime) + " " + str(scandata))
    with open(g_log_path, mode='a') as log_file:
        csv_writer = csv.writer(log_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([g_thread_num,engine, sqlid, costtime,scandata])


if(__name__ == "__main__"):
    init_sql_collections()
    local_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()) 
    g_log_path = os.path.join(os.getcwd(), local_time+".csv")
    g_athena_client = boto3.client('athena')
    print(g_thread_num)
    for i in range(g_thread_num):
        wthread = workThread(i,"work-thread-" + str(i))
        wthread.start()
