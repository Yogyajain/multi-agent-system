from langchain_openai import ChatOpenAI
import os
from getpass import getpass
from dotenv import load_dotenv
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableMap, RunnableLambda
import pickle
import re
from langgraph.graph import StateGraph, START, END
from typing import Dict, Any, TypedDict, Annotated
from operator import add
from helper import *
from IPython.display import Image

with open('kb3.pkl', 'rb') as f:
    loaded_dict = pickle.load(f)


class overallstate(TypedDict):
    user_query:str
    table=str
    table_extract=Annotated[list[str], add]
    
def column_node(state:overallstate):
    q=state['user_query']
    table_extract=state['table_extract']
    inter=[]
    final_col=[]
    for t in table_extract:
        table_name=table_extract[0]
        sub_query=table_extract[1]
         # gives all the columns
        tab=loaded_dict[table_name][table_name] # gives description of table
        columns=tab["columns"]
        out_column=chain_column_extractor.invoke({"main_question":q,"columns":columns,"query":sub_query})
        trans_col = eval(out_column)
        for col_selec in trans_col:
            new_col = ["name of table:" + table_name] + col_selec
            inter.append(new_col)
        final_col.extend(inter)
    return {"column_extract": final_col}

def sq_node(state:overallstate):
    q=state['user_query']
    table_list=state['table_list']
    result_dict = {}
    for table in table_list:
        tab=loaded_dict[table][table] # gives description of table
        table_description_value = tab['table_description']
        result_dict[table] = table_description_value
    res=chain_subquestion.invoke({"user_query":q,"tables":str(result_dict)})
    return {"table_extact":eval(res)}

builder_final=StateGraph(overallstate)
builder_final.add_node("subquestion_node",sq_node)
builder_final.add_node("column_node",column_node)
builder_final.add_edge(START,"subquestion_node")
builder_final.add_edge("subquestion_node","column_node")
builder_final.add_edge("column_node",END)
graph_final=builder_final.compile()