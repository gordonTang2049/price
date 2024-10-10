import textwrap as tw
import pandas as pd

class Sql_op:
    def __init__(self):
        return

    def get_code(self, curs: object):

        code = []

        STATEMENT = "Select TRIM([TICKER]) FROM [FINANCE].[DBO].[metadata] ORDER BY [TICKER]"

        curs.execute(STATEMENT)

        for index, row in enumerate(curs.fetchall()):
            # if index != 0:
            code.append(row[0])

        return code

    def update_metadataNoPrice(
            self,
            curs: object,
            code: str):
        STATEMENT = f"UPDATE [FINANCE].[DBO].[metadata] SET [hasPriceInfo] = 0 WHERE [TICKER] = '{code}'"
        curs.execute(STATEMENT)

    def get_insert_statement(
            self,
            curs: object,
            TB_name: str):

        STATEMENT = "\
        SELECT\
            c.name 'Column Name'\
        FROM\
            sys.columns c\
        WHERE\
            c.object_id = OBJECT_ID(N'price')\
        "
        cols_name = []
        q_marks = []

        curs.execute(STATEMENT)

        for index, row in enumerate(curs.fetchall()):
            # if index != 0:
            cols_name.append(row[0])

        cols_name = [str(tw.wrap(name)).replace("'", "") for name in cols_name]

        cols_name_str = ",".join(cols_name)

        for value in range(len(cols_name)):
            q_marks.append("?")

        q_marks_str = ",".join(q_marks)

        statement = f"INSERT INTO [FINANCE].[dbo].[{TB_name}] ({cols_name_str}) VALUES ({q_marks_str})"

        return statement


    def insert_data(
            self,
            curs: object,
            insert_statement: str,
            df: object):


        shareids = tuple(df.SharePrice_id.to_numpy(dtype=str))
        print(shareids)

        existing_ids_query = f"""
        SELECT [SharePrice_id]
        FROM [FINANCE].[dbo].[PRICE]
        WHERE [SharePrice_id] IN ({', '.join(['?'] * len(shareids))})
        """

        
        curs.execute(existing_ids_query, shareids)

        existing_ids = {row[0] for row in curs.fetchall()}
        
        df_to_insert = df[~df['SharePrice_id'].isin(existing_ids)]


        if not df_to_insert.empty:
            
            print(df_to_insert)

            # curs.fast_executemany = True

            # df or df_to_insert
            # curs.executemany(
            #     insert_statement, 
            #     list(df_to_insert.itertuples(index=False, name=None))
            # )

            # df or df_to_insert
            
            for index, row in df_to_insert.iterrows():
                        # print(row[0])
                        curs.execute(insert_statement,
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7])