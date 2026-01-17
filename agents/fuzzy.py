import pandas as pd
from sqlalchemy import create_engine,  text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Integer, Float, String
from rapidfuzz import process, fuzz
from knowledge_base.info import run_sql_query

def get_values(table,column):
    query=f"SELECT DISTINCT {column} FROM {table}"
    df=run_sql_query(query)
    r=[c[column] for c in df]
    return r

def get_best_match(subval,unq_col_val):
    best_match,score,index=process.extractOne(subval,unq_col_val,score_cutoff=80)
    return best_match,score

def call_match(val):
    final=[]
    for i in val[1:]:
        table=i[0]
        column=i[1]
        str_lst=[i.strip() for i in i[2].split(',')]

        unq_col_val=get_values(table,column)
        unq_col_val=[str(i) for i in unq_col_val]
        
        for subval in str_lst:
            best_match,score=get_best_match(subval,unq_col_val)
            final.append({"table_name": table, "column_name":column, "filter_value":best_match})
        return final