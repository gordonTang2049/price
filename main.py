import os
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime, timedelta, date
import yfinance as yf

from typing import List
import random
from sql_op import Sql_op
from cleaning import Cleaning


server = os.environ['SERVER_NAME']
database = os.environ['DB_NAME']
username = os.environ['USER']
password = os.environ['DB_PASSWORD']

sql = Sql_op()
clean = Cleaning()

sql_server = "FreeTDS"
# sql_server = "SQL Server"
tableName = 'PRICE'

def main():


    cnxn = pyodbc.connect('DRIVER={'+sql_server+'};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password +';TrustServerCertificate=yes;', autocommit=True)
    cursor = cnxn.cursor()

    chunks = pd.read_sql('''SELECT
    *
    FROM 
        [FINANCE].[DBO].[METADATA] META
    LEFT JOIN
    (
            SELECT
                [_TICKER], 
                MAX([_DATE]) AS LAST_DATE_DB
            FROM 
                [FINANCE].[DBO].[PRICE] 
            GROUP BY
                [_TICKER]
    ) PRICE
    ON 
        META.[TICKER] = PRICE.[_TICKER]
    WHERE 
        META.[hasPriceInfo] = 1 
    ORDER BY
        META.[TICKER]''',cnxn, chunksize=1000)


    # df = pd.read_sql('''SELECT
    # *
    # FROM 
    #     [FINANCE].[DBO].[METADATA] META
    # LEFT JOIN
    # (
    #         SELECT
    #             [_TICKER], 
    #             MAX([_DATE]) AS LAST_DATE_DB
    #         FROM 
    #             [FINANCE].[DBO].[PRICE] 
    #         GROUP BY
    #             [_TICKER]
    # ) PRICE

    # ON 
    #     META.[TICKER] = PRICE.[_TICKER]
    # WHERE 
    #     META.[hasPriceInfo] = 1 
    # AND 
    #     META.[TICKER] NOT IN (
    #     SELECT 
    #         DISTINCT [_TICKER]
    #     FROM 
    #         [FINANCE].[DBO].[PRICE] 
    #     WHERE 
    #         [_DATE] = '2024-10-07'
    #     )
    # ORDER BY
    #     META.[TICKER] 
    # DESC''',cnxn)
    
    df = pd.concat(chunks)
    
    df_hasLastDate = df[~df['LAST_DATE_DB'].isnull()]
    
    df_hasNoLastDate = df[df['LAST_DATE_DB'].isnull()]

    df_hasLastDate.LAST_DATE_DB = pd.to_datetime(df_hasLastDate.LAST_DATE_DB.str.split(' ' ,expand=True)[0], format='%Y-%m-%d')
    
    # has share price last date in DB
    # =====================================================================
    for i in range(df_hasLastDate.shape[0]):
        
        lastDate = df_hasLastDate.iloc[i,:].LAST_DATE_DB + timedelta(days=1)

        ticker = df_hasLastDate.iloc[i,:].TICKER

        hasData, data = clean.yf_df_date(ticker, lastDate.strftime("%Y-%m-%d"))

        if hasData:
            
            STATEMENT = sql.get_insert_statement(cursor, tableName)

            sql.insert_data(cursor, STATEMENT, data)

        else:
            
            hasData, data = clean.yf_df_day(ticker, '1d')
            
            if hasData:
                STATEMENT = sql.get_insert_statement(cursor, tableName)
                
                print(STATEMENT)
                
                sql.insert_data(cursor, STATEMENT, data)
            else:
                print(ticker, 'Has No Share Data')


    # =====================================================================
    for i in range(df_hasNoLastDate.shape[0]):

        ticker = df_hasLastDate.iloc[i,:].TICKER

        hasData, data = clean.yf_df_day(ticker, '1d')

        if hasData:

            STATEMENT = sql.get_insert_statement(cursor, tableName)
            
            sql.insert_data(cursor, STATEMENT, data)

        else:

            print(ticker, 'Has No Share Data')

    cnxn.commit()
    cursor.close()
    cnxn.close()

main()

# todayDate = date.today().strftime("%Y.%m.%d")
# current_date = round(datetime.datetime.now().timestamp())
# previous_date = round((datetime.datetime.today() - datetime.timedelta(days=1)).timestamp())
