import os
import snowflake.connector
import pandas as pd

from pickle import TRUE
from sqlite3 import Cursor

from datetime import datetime as dt
import numpy as np

#query1-the input needs to be tweaked
ctx = snowflake.connector.connect(
    role='ROLE',
    user='USER',
    password=os.environ.get('SNOW_PASSWORD'),
    account='SNOWFLAKE_LINK',
    warehouse='PROD_WAREHOUSE',
    database='PROD_DATABASE',
    schema='SCHEMA'
    )

cur = ctx.cursor() #a cursor project created

#The following function is where we calculate summary statistics right after data pull from Snowflake table
def func(As_of_dt):
    ##first metric
    cur.execute("SQL statement;")
    data = cur.fetchall()
    cols = []
    for i,_ in enumerate(cur.description):
        cols.append(cur.description[i][0])
    
    df=pd.DataFrame(np.array(data),columns=cols)
    df = df.pivot(index='date', columns='category', values='CNT')
    
    ##second metric
    cur.execute("SQL statement2;")
    data = cur.fetchall()
    cols = []
    for i,_ in enumerate(cur.description):
        cols.append(cur.description[i][0])
    
    df2=pd.DataFrame(np.array(data),columns=cols)
    df2 = df2.pivot(index='date', columns='category', values='CNT')
    
    ##third metric
    cur.execute("SQL statement3;")
    data = cur.fetchall()
    cols = []
    for i,_ in enumerate(cur.description):
        cols.append(cur.description[i][0])
    
    df3=pd.DataFrame(np.array(data),columns=cols)
    df3 = df3.pivot(index='date', columns='category', values='CNT')
    
    ###terminate the connection
    cur.close()
    
    ###Combined grid for the final daily reporting
    #df =df[df.index.dayofweek < 5]
    df=df.reset_index()
    df.fillna(0, inplace=True)
    df =df.loc[df['date'] <= dt.strptime(As_of_dt, '%Y-%m-%d')]

    df['metric1'] = df['a']-df['b']
    df['metric2'] = df['c']-df['d']
    df['metric3'] = df['e']+df['f']
    
    
    
    df['7MA_a'] = df.a.rolling(7,min_periods=1).mean().round(decimals=0)
    df['14MA_a'] = df.a.rolling(14,min_periods=1).mean().round(decimals=0)
    df['7MA_b'] = df.b.rolling(7,min_periods=1).mean().round(decimals=0)
    df['14MA_b'] = df.b.rolling(14,min_periods=1).mean().round(decimals=0)
    df['7MA_c'] = df.c.rolling(7,min_periods=1).mean().round(decimals=0)
    df['14MA_c'] = df.c.rolling(14,min_periods=1).mean().round(decimals=0)
    df['7MA_d'] = df.d.rolling(7,min_periods=1).mean().round(decimals=0)
    df['14MA_d'] = df.d.rolling(14,min_periods=1).mean().round(decimals=0)
    df['7MA_e'] = df.e.rolling(7,min_periods=1).mean().round(decimals=0)
    df['14MA_e'] = df.e.rolling(14,min_periods=1).mean().round(decimals=0)
    
    df['month'] = df['date'].dt.month
    
    df_M=pd.DataFrame(df.groupby(['month'])['a','b','c','d','e'].mean().round(decimals=0))
    df_M=df_M.reset_index()
    df_M.columns= ['month','MA_a','MA_b','MA_c','MA_d','MA_e']
    
    df['quarter'] = df['date'].dt.quarter
    df_Q=pd.DataFrame(df.groupby(['quarter'])['a','b','c','d','e'].mean().round(decimals=0))
    df_Q=df_Q.reset_index()
    df_Q.columns= ['quarter','QA_a','QA_b','QA_c','QA_d','QA_e']
    
    df=pd.merge(pd.merge(df,df_M, on='month'),df_Q, on='quarter')

    df['year'] = df['date'].dt.year
    df_Y=pd.DataFrame(df.groupby(['year'])['a','b','c','d','e'].mean().round(decimals=0))
    df_Y=df_Y.reset_index()
    df_Y.columns= ['year','YA_a','YA_b','YA_c','YA_d','YA_e']

    df=pd.merge(df,df_Y, on='year')

    df2=df2.reset_index()
    df2.fillna(0, inplace=True)
    df2 =df2.loc[df2['date_i'] <= dt.strptime(As_of_dt, '%Y-%m-%d')]
    
    df2['7MA_i'] = df2.a.rolling(7,min_periods=1).mean().round(decimals=0)
    df2['14MA_i'] = df2.a.rolling(14,min_periods=1).mean().round(decimals=0)
    
    df2['month'] = df2['date_i'].dt.month
    df2['quarter'] = df2['date_i'].dt.quarter
    df2['year'] = df2['date_i'].dt.year
    df_M=pd.DataFrame(df2.groupby(['month'])['a'].mean().round(decimals=0))
    df_M=df_M.reset_index()
    df_M.columns= ['month','MA_i']
    df2=pd.merge(df2,df_M, on='month')
    df_Q=pd.DataFrame(df2.groupby(['quarter'])['a'].mean().round(decimals=0))
    df_Q=df_Q.reset_index()
    df_Q.columns= ['quarter','QA_i']
    df2=pd.merge(df2,df_Q, on='quarter')
    df_Y=pd.DataFrame(df2.groupby(['year'])['a'].mean().round(decimals=0))
    df_Y=df_Y.reset_index()
    df_Y.columns= ['year','YA_i']
    df2=pd.merge(df2,df_Y, on='year')

    df3=df3.reset_index()
    df3.fillna(0, inplace=True)
    df3 =df3.loc[df3['date_c'] <= dt.strptime(As_of_dt, '%Y-%m-%d')]
    
    df3['7MA_c'] = df3.a.rolling(7,min_periods=1).mean().round(decimals=0)
    df3['14MA_c'] = df3.a.rolling(14,min_periods=1).mean().round(decimals=0)
    
    df3['month'] = df3['date_c'].dt.month
    df3['quarter'] = df3['date_c'].dt.quarter
    df3['year'] = df3['date_c'].dt.year
    df_M=pd.DataFrame(df3.groupby(['month'])['a'].mean().round(decimals=0))
    df_M=df_M.reset_index()
    df_M.columns= ['month','MA_c']
    df3=pd.merge(df3,df_M, on='month')
    df_Q=pd.DataFrame(df3.groupby(['quarter'])['a'].mean().round(decimals=0))
    df_Q=df_Q.reset_index()
    df_Q.columns= ['quarter','QA_c']
    df3=pd.merge(df3,df_Q, on='quarter')
    df_Y=pd.DataFrame(df3.groupby(['year'])['a'].mean().round(decimals=0))
    df_Y=df_Y.reset_index()
    df_Y.columns= ['year','YA_c']
    df3=pd.merge(df3,df_Y, on='year')

    f1 = pd.DataFrame(df2.loc[df2['date_i']==As_of_dt],columns=['date_i','a','7MA_i','14MA_i','MA_i','QA_i','YA_i'])
    f2 = pd.DataFrame(df3.loc[df3['date_c']==As_of_dt],columns=['date_c','a','7MA_c','14MA_c','MA_c','QA_c','YA_c'])
    f3 = pd.DataFrame(df.loc[df['date']==As_of_dt],columns=['date','a','7MA_a','14MA_a','MA_a','QA_a','YA_a'])
    f4 = pd.DataFrame(df.loc[df['date']==As_of_dt],columns=['date','b','7MA_b','14MA_b','MA_b','QA_b','YA_b'])
    f5 = pd.DataFrame(df.loc[df['date']==As_of_dt],columns=['date','c','7MA_c','14MA_c','MA_c','QA_c','YA_c'])
    f6 = pd.DataFrame(df.loc[df['date']==As_of_dt],columns=['date','d','7MA_d','14MA_d','MA_d','QA_d','YA_d'])
    f7 = pd.DataFrame(df.loc[df['date']==As_of_dt],columns=['date','e','7MA_e','14MA_e','MA_e','QA_e','YA_e'])
    f1.columns=['As_of_Date','Current Day','7DMA','14DMA','Current Month Average','Current Quarter Average','Current Year Average']
    f2.columns=['As_of_Date','Current Day','7DMA','14DMA','Current Month Average','Current Quarter Average','Current Year Average']
    f3.columns=['As_of_Date','Current Day','7DMA','14DMA','Current Month Average','Current Quarter Average','Current Year Average']
    f4.columns=['As_of_Date','Current Day','7DMA','14DMA','Current Month Average','Current Quarter Average','Current Year Average']
    f5.columns=['As_of_Date','Current Day','7DMA','14DMA','Current Month Average','Current Quarter Average','Current Year Average']
    f6.columns=['As_of_Date','Current Day','7DMA','14DMA','Current Month Average','Current Quarter Average','Current Year Average']
    f7.columns=['As_of_Date','Current Day','7DMA','14DMA','Current Month Average','Current Quarter Average','Current Year Average']
    summary = pd.concat([pd.concat([pd.concat([pd.concat([pd.concat([pd.concat([f1,f2]),f3]),f4]),f5]),f6]),f7])
    summary.insert(1, "metrics", ['i','a', 'b','c', 'd','e'], True)

    summary.to_csv("summary_{As_of_dt}.csv".format(As_of_dt=As_of_dt),index=False)
    
    return summary

#On Monday, put the last Fri's date; otherwise, put the date of yesterday
As_of_dt='2024-02-13'
func(As_of_dt)
