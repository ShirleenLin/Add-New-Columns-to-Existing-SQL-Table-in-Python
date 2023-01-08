import tushare as ts
import time, datetime, sqlite3, logging
from tqdm import tqdm
import pandas as pd
from datetime import date
import numpy as np


def downLoadData(pro):

    start_date = "2016-04-28"
    end_date="2023-01-03"

    conn = sqlite3.connect("New_Stk_hist.sqlite")
    cursor = conn.cursor()
    #Create a temporary table
    sql = "CREATE TABLE IF NOT EXISTS Add_Index (ts_code TEXT, ann_date TEXT,  total_revenue REAL, revenue REAL, UNIQUE(ts_code, ann_date))"
    cursor.execute(sql)

    #breakpoint recovery
    try:
      sql = '''SELECT append_codelist FROM Append_Progress'''
      cursor.execute(sql)
    except:
      sql = "CREATE TABLE IF NOT EXISTS Append_Progress(append_codelist)"
      cursor.execute(sql)
    append_codelist = cursor.fetchall()

    #get codelist
    codelist = pro.stock_basic()

    for i in tqdm(range(len(codelist["ts_code"]))): #1st loop: grab a code
        if (codelist["ts_code"][i],) in append_codelist: continue
        else:
            try:
                df = pro.income(ts_code=codelist["ts_code"][i],start_date=start_date, end_date=end_date)
                print(codelist["ts_code"][i],"added to append_codelist[]")
                c_len = df.shape[0]  #Number of days
            except:
                print("Failed to download", codelist["ts_code"][i])
                continue
        for j in range(c_len): #2nd loop: grab a row (day)
              resu0 = list(df.iloc[c_len-1-j])
              resu = []
              for k in range(len(resu0)): #3rd loop: grab a column (index)
                 if str(resu0[k]) == "nan": resu.append(np.nan)
                 else:
                   resu.append(resu0[k])
              state_dt = (datetime.datetime.strptime(resu[1],"%Y%m%d")).strftime("%Y-%m-%d")
              sql = "INSERT OR IGNORE INTO Add_Index(ann_date, ts_code,total_revenue, revenue) VALUES ('%s','%s','%.2f','%.2f')" % (state_dt,str(resu[0]),float(resu[9]),float(resu[10]))
              try:
                  cursor.execute(sql)
                  conn.commit()
              except Exception as err:
                  print("Error:", err)
                  print("Failed to insert:", codelist["ts_code"][i])
      sql = "INSERT OR IGNORE INTO Append_Progress(append_codelist) VALUES ('%s')" % (codelist["ts_code"][i])
      cursor.execute(sql)
      print(codelist["ts_code"][i],"added to append_codelist[]")
    print("Successfully downloaded new data into Add_Index Table")
    sql = '''SELECT * FROM Financial LEFT JOIN Add_Index ON Add_Index.ann_date=Financial.ann_date AND Add_Index.ts_code=Financial.ts_code'''
    cursor.execute(sql)
    print("Successfully merged new data into the existing Financial Table") #Depending on the need, you can also delete Add_Index Table
    conn.commit()
    conn.close()
#You need to get your own Tushare token from https://tushare.pro/document/1?doc_id=39
ts.set_token('')
pro = ts.pro_api()
downLoadData(pro)