#!/usr/bin/python3

import random
import os

g_sql_num = 35
g_sql_collections = []

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

if(__name__ == "__main__"):
    init_sql_collections()
    sql_path = os.path.join(os.getcwd(), "sql")
    for i in range(35,301):
        sql_file = os.path.join(sql_path,str(i) + ".sql")
        org_idx = random.randrange(len(g_sql_collections))
        f = open(sql_file, "w")
        f.write(g_sql_collections[org_idx])
        f.close()
