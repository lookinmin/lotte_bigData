import pandas
from pyarrow import csv
import pymysql

file = "C:\\Users\\33387\\Desktop\\LPOINT_BIG_COMP_06_LPAY.csv"

pyarrow_table = csv.read_csv(file)
df = pyarrow_table.to_pandas()

# MySQL Connection 연결
con = pymysql.connect(host='localhost', user='root', password='root', charset='utf8')  # 한글처리 (charset = 'utf8')


# DB 및 테이블 생성
cur = con.cursor()
cur.execute("create database if not exists big_data")
make_table_sql1="CREATE TABLE if not exists big_data.demo (`cust` VARCHAR(10) NOT NULL,`ma_fem_dv` TINYINT NULL,`ages` TINYINT NULL,`zon_hlv` VARCHAR(3) NULL,`num` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`num`));"
cur.execute(make_table_sql1)
make_table_sql2="CREATE TABLE if not exists big_data.pdde (`cust` VARCHAR(10) NOT NULL,`rct_no` VARCHAR(12) NULL,`chnl_dv` TINYINT NULL," \
                "`cop_c` VARCHAR(3) NULL,`br_c` VARCHAR(7) NULL, `pd_c` VARCHAR(6) NULL,`de_dt` VARCHAR(8) NULL,`de_hr` TINYINT NULL," \
                "`buy_am` INT UNSIGNED NULL,`buy_ct` MEDIUMINT UNSIGNED NULL,`num` INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (`num`));"
cur.execute(make_table_sql2)
make_table_sql3="CREATE TABLE if not exists big_data.cop_u (" \
                "`cust` VARCHAR(10) NOT NULL," \
                "`rct_no` VARCHAR(18) NULL," \
                "`cop_c` VARCHAR(3) NULL," \
                "`br_c` VARCHAR(7) NULL, " \
                "`chnl_dv` TINYINT NULL," \
                "`de_dt` VARCHAR(8) NULL," \
                "`vst_dt` VARCHAR(8) NULL,"\
                "`de_hr` TINYINT NULL," \
                "`buy_am` INT UNSIGNED NULL," \
                "`num` INT NOT NULL AUTO_INCREMENT," \
                " PRIMARY KEY (`num`));"
cur.execute(make_table_sql3)
make_table_sql4="CREATE TABLE if not exists big_data.pd_clac (" \
                "`pd_c` VARCHAR(6) NOT NULL," \
                "`pd_nm` VARCHAR(22) NULL," \
                "`clac_hlv_nm` VARCHAR(12) NULL," \
                "`clac_mcls_nm` VARCHAR(15) NULL," \
                "`num` INT NOT NULL AUTO_INCREMENT," \
                "PRIMARY KEY (`num`));"
cur.execute(make_table_sql4)
make_table_sql5="CREATE TABLE if not exists big_data.br (" \
                "`br_c` VARCHAR(7) NOT NULL," \
                "`cop_c` VARCHAR(3) NULL," \
                "`zon_hlv` VARCHAR(3) NULL," \
                "`zon_mcls` VARCHAR(6) NULL," \
                "`num` INT NOT NULL AUTO_INCREMENT," \
                "PRIMARY KEY (`num`));"
cur.execute(make_table_sql5)
make_table_sql6="CREATE TABLE if not exists big_data.lpay (" \
                "`cust` VARCHAR(10) NOT NULL," \
                "`rct_no` VARCHAR(16) NULL," \
                "`cop_c` VARCHAR(3) NULL," \
                "`chnl_dv` TINYINT NULL," \
                "`de_dt` VARCHAR(8) NULL," \
                "`de_hr` TINYINT NULL," \
                "`buy_am` INT UNSIGNED NULL," \
                "`num` INT NOT NULL AUTO_INCREMENT," \
                " PRIMARY KEY (`num`));"
cur.execute(make_table_sql6)
con.commit()

# num=0
# for index, row in df.iterrows():
#     cust=row.cust
#     if(row.ma_fem_dv=='여성'):
#         sex=1
#     else:
#         sex=0
#     num+=1
#     age=row.ages
#     age=int(age[0:2])
#     produc=row.zon_hlv
#     sql = "insert into big_data.demo values(%s, %s,%s,%s,%s)"
#     val = (cust, sex, age, produc,num)
#     cur.execute(sql, val)
# print(df)
# con.commit()

# num=0
# for index, row in df.iterrows():
#     num+=1
#     sql = "insert into big_data.pdde values(%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s)"
#     val = (row.cust, row.rct_no, int(row.chnl_dv), row.cop_c,row.br_c,row.pd_c,row.de_dt,int(row.de_hr),int(row.buy_am),int(row.buy_ct),num)
#     cur.execute(sql, val)
#     if(num%5000==0):
#         con.commit()
# print(df)
# con.commit()

# num=0
# for index, row in df.iterrows():
#     num+=1
#     sql = "insert into big_data.cop_u values(%s, %s,%s,%s,%s,%s, %s,%s,%s,%s)"
#     val = (row.cust, row.rct_no, row.cop_c,row.br_c,int(row.chnl_dv),row.de_dt,row.vst_dt,int(row.de_hr),int(row.buy_am),num)
#     cur.execute(sql, val)
#     if(num%10000==0):
#         print("commit")
#         con.commit()
# print(df)
# con.commit()

# num=0
# for index, row in df.iterrows():
#     num+=1
#     sql = "insert into big_data.pd_clac values(%s, %s,%s,%s,%s)"
#     val = (row.pd_c, row.pd_nm, row.clac_hlv_nm,row.clac_mcls_nm,num)
#     cur.execute(sql, val)
#     if(num%10000==0):
#         print("commit")
#         con.commit()
# print(df)
# con.commit()


# num=0
# for index, row in df.iterrows():
#     num+=1
#     sql = "insert into big_data.br values(%s, %s,%s,%s,%s)"
#     val = (row.br_c, row.cop_c, row.zon_hlv,row.zon_mcls,num)
#     cur.execute(sql, val)
#     if(num%10000==0):
#         print("commit")
#         con.commit()
# print(df)
# con.commit()

# num=0
# for index, row in df.iterrows():
#     num+=1
#     sql = "insert into big_data.lpay values(%s, %s,%s,%s,%s,%s,%s,%s)"
#     val = (row.cust, row.rct_no, row.cop_c,row.chnl_dv,row.de_dt,row.de_hr,row.buy_am,num)
#     cur.execute(sql, val)
#     if(num%10000==0):
#         print("commit")
#         con.commit()
# print(df)
# con.commit()

# STEP 5: DB 연결 종료
con.close()
