import pandas as pd
from pyarrow import csv
import pymysql

file = "C:\\Users\\EunJeong\\Desktop\\빅데이터공모전\\LPOINT_BIG_COMP_02_PDDE.csv"

pyarrow_table = csv.read_csv(file)
df = pyarrow_table.to_pandas()

# STEP 2: MySQL Connection 연결
con = pymysql.connect(host='localhost', user='root', password='1234',
                      db='data', charset='utf8')  # 한글처리 (charset = 'utf8')

# STEP 3: Connection 으로부터 Cursor 생성
cur = con.cursor()
num=0
for index, row in df.iterrows():
    cust=row.cust
    if(row.ma_fem_dv=='여성'):
        sex=1
    else:
        sex=0
    age=row.ages
    age=int(age[0:2])
    produc=row.zon_hlv
    num+=1
    sql = "insert into demo values(%s, %s,%s,%s)"
    val = (cust, sex, age, produc)
    cur.execute(sql, val)
print(df)
con.commit()

# STEP 5: DB 연결 종료
con.close()
