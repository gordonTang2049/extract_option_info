import textwrap as tw
import numpy as np
class Sql_op:
    def __init__(self):
        return
    
    def get_insert_statement(
        self,
        curs : object,
        tablename : str):

        STATEMENT = "\
        SELECT\
            c.name 'Column Name'\
        FROM\
            sys.columns c\
        WHERE\
            c.object_id = OBJECT_ID(N'option')\
        "
        cols_name = []
        q_marks = []

        curs.execute(STATEMENT)
    
        for index, row in enumerate(curs.fetchall()):
            if index != 0:
                cols_name.append(row[0])
        
        cols_name = [str(tw.wrap(name)).replace("'", "") for name in cols_name]

        cols_name_str = ",".join(cols_name)

        for value in range(len(cols_name)):
            q_marks.append("?")
        
        q_marks_str = ",".join(q_marks)
        
        statement = f"INSERT INTO [FINANCE].[dbo].[{tablename}] ({cols_name_str}) VALUES ({q_marks_str})"

        return statement


    def insert_data(
        self,
        curs : object,
        insert_statement : str,
        df : object ):
        
        # curs.fast_executemany = True
        # batch_size = 10
        # for chunk in np.array_split(df, batch_size):
        #     curs.executemany(insert_statement,
        #                         chunk.values.tolist()
        #                         # list(chunk.itertuples(index=False, name=None))
        #                     )
        # curs.executemany(insert_statement,list(df.itertuples(index=False, name=None)))

        for index, row in df.iterrows():
            # print(row[0])
            curs.execute(insert_statement,
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[11],
            row[12],
            row[13],
            row[14],
            row[15],
            row[16],
            row[17],
            row[18])


    # def truncateTable(
    #     self,
    #     curs : object,
    #     TB_name : str):

    #     sqlStatement = f"TRUNCATE TABLE [JLL].[dbo].[{TB_name}]"
    #     curs.execute(sqlStatement)

    # def update_metadataNoOption(self, curs : object, code : str):
    #     STATEMENT = f"UPDATE [FINANCE].[DBO].[metadata] SET [hasOptionChain] = 0 WHERE [TICKER] = '{code}'"
    #     curs.execute(STATEMENT)

    # def update_metadatahasOption(self, curs : object, code : str):
    #     STATEMENT = f"UPDATE [FINANCE].[DBO].[metadata] SET [hasOptionChain] = 1 WHERE [TICKER] = '{code}'"
    #     curs.execute(STATEMENT)