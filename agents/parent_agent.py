from langgraph.graph import StateGraph, START, END
from typing import Dict, Any, TypedDict, Annotated
from operator import add
import pickle
from IPython.display import Image

from agents.helper import agent_2
from agents.sub_agent import graph_final
from langgraph.constants import Send
import os
from agents.helper import chain_filter_extractor, chain_query_extractor, chain_query_validator
from agents.fuzzy import call_match
from datetime import datetime
import json
import tqdm


import pandas as pd
db_store = {
    "DB_ShopCore" : ['Users ', 'Products','Orders'],
    "DB_ShipStream" : ['Shipments', 'Warehouses', 'TrackingEvents'],
    "DB_PayGuard": ["Wallets", "Transactions","PaymentMethods"],
    "DB_CareDesk":['Tickets',"TicketMessages","SatisfationSurveys"]
}


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KB_PATH = os.path.join(BASE_DIR, "knowledge_base", "kb4.pkl")

with open(KB_PATH, "rb") as f:
    kb = pickle.load(f)

def remove_duplicates(f):
    s = set()
    final = []
    for k, v in f.items():
        if k in ['shopcore_out','shipstream_out','payguard_out','caredesk_out']:
            for item in v['column_extract']:
                key = tuple(item)
                if key not in s:
                    final.append(item)
                    s.add(key)
    return final


class finalstate(TypedDict):
    user_query: str
    sub_agent: list[str]
    filter_extract: list[str]
    filtered_col: list[str]
    fuzzy_match: list[str]
    shopcore_out: dict
    shipstream_out: dict
    payguard_out: dict
    caredesk_out: dict
    sql_query: str

def parent(state:finalstate):
    print("parent node")
    q=state['user_query']
    res=agent_2(q)
    print(res)
    # return {}
    return {"sub_agent":eval(res)}

def request(state:finalstate):
    sub_agents=state['sub_agent']
    print("Routed request to"+str(sub_agents)+' agents')
    return sub_agents

def ShopCore(state:finalstate):
    q=state['user_query']
    print("Extracting relavant tables and columns from shopcore agent................")
    sub=graph_final.invoke({"user_query":q,"table_list":db_store['DB_ShopCore']})
    print("response by shopcore agent",sub)
    return {"shopcore_out":sub}

def ShipStream(state:finalstate):
    q=state['user_query']
    print("Extracting relavant tables and columns from shipstream agent................")
    sub=graph_final.invoke({"user_query":q,"table_list":db_store['DB_ShipStream']})
    print("response by shipstream agent",sub)
    return {"shipstream_out":sub}

def PayGuard(state:finalstate):
    q=state['user_query']
    print("Extracting relavant tables and columns from payguard agent................")
    sub=graph_final.invoke({"user_query":q,"table_list":db_store['DB_PayGuard']})
    print("response by paygaurd agent",sub)
    return {"payguard_out":sub}


def CareDesk(state:finalstate):
    q=state['user_query']
    print("Extracting relavant tables and columns from caredesk agent................")
    sub=graph_final.invoke({"user_query":q,"table_list":db_store['DB_CareDesk']})
    print("response by caredesk agent",sub)
    return {"caredesk_out":sub}

def filter_condition(state:finalstate):
    f=state['filter_extract']
    if len(f)==1:
        return "query_generator"
    else:
        return "fuzzy_match"

def filter_check(state:finalstate):
    q=state['user_query']
    f={}
    col_f=[]
    for k in ['shopcore_out','shipstream_out','payguard_out','caredesk_out']:
        if k in state:
            f[k]=state[k]
            col_f.extend(state[k]['column_extract'])
    col_details = remove_duplicates(f)
    print("Checking the need for filter................")
    res=chain_filter_extractor.invoke({"query":q,"columns":str(col_details)})
    print({"filter_extract":eval(res),"filtered_col":str(col_details)})
    return {"filter_extract":eval(res),"filtered_col":str(col_details)}

def fuzzy_match(state:finalstate):
    filter_extract=state['filter_extract']
    print("Checking the fuzzy match................")
    out_fuzzy=call_match(filter_extract)
    print("done filtering................")
    print("response from fuzzy:",out_fuzzy)
    return {"fuzzy_match":out_fuzzy}

def query_generator(state:finalstate):
    q=state['user_query']
    table_col=state['filtered_col']
    if state.get('fuzzy_match'):
        filter=state['fuzzy_match']
    else:
        filter=''
    print("Generating the query................")
    final_query=chain_query_extractor.invoke({"query":q,"columns":table_col,"filters":filter})
    print("qeury:",final_query)
    return {"sql_query":final_query}

def query_validator(state:finalstate):
    q=state['user_query']
    sql_query=state['sql_query']
    table_col=state['filtered_col']
    if state.get('fuzzy_match'):
        filter=state['fuzzy_match']
    else:
        filter=''
    print("Validating the query................")
    out_validator=chain_query_validator.invoke({"query":q,"sql_query":sql_query,"columns":table_col,"filters":filter})
    # print("respnonse from query validator",out_validator)
    return {"query_validator":out_validator}

builder_final = StateGraph(finalstate)
builder_final.add_node("parent", parent)
builder_final.add_node("ShopCore", ShopCore)
builder_final.add_node("ShipStream", ShipStream)
builder_final.add_node("PayGuard", PayGuard)
builder_final.add_node("CareDesk", CareDesk)
builder_final.add_node("filter_check", filter_check)
builder_final.add_node("fuzzy_match", fuzzy_match)
builder_final.add_node("query_generator", query_generator)
builder_final.add_node("query_validator", query_validator)
builder_final.add_edge(START, "parent")
builder_final.add_conditional_edges("parent",request,["ShopCore","ShipStream","PayGuard","CareDesk"])
builder_final.add_edge("ShopCore", "filter_check")
builder_final.add_edge("ShipStream", "filter_check")
builder_final.add_edge("PayGuard", "filter_check")
builder_final.add_edge("CareDesk", "filter_check")
builder_final.add_conditional_edges("filter_check",filter_condition,["query_generator","fuzzy_match"])
builder_final.add_edge("fuzzy_match", "query_generator")
builder_final.add_edge("query_generator", "query_validator")
builder_final.add_edge("query_validator", END)
graph_main=builder_final.compile()
