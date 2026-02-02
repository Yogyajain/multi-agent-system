from knowledge_base.info import run_sql_query,execute_query
import os
import pickle
from agents.fuzzy import call_match,get_best_match
q="""SELECT 
    t1.UserID,
    t1.Name,
    t2.OrderID,
    t2.UserID,
    t2.ProductID,
    t2.OrderDate,
    t2.Status,
    t3.ProductID,
    t3.Name,
    t3.Price
FROM Users AS t1
INNER JOIN Orders AS t2 ON t1.UserID = t2.UserID
INNER JOIN Products AS t3 ON t2.ProductID = t3.ProductID
WHERE t1.Name = 'Sarah Johnson';"""
resp=run_sql_query(q)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.join(BASE_DIR,"multi-agent-system")
KB_PATH = os.path.join(BASE_DIR, "knowledge_base", "kb5.pkl")

with open(KB_PATH, 'rb') as f:
    loaded_dict = pickle.load(f)

# print(loaded_dict)
tab=loaded_dict['SatisfationSurveys']['SatisfationSurveys'] # gives description of table

raw=run_sql_query("SELECT name FROM sqlite_master WHERE type='table'")
# print(tab)
trans_col={'selected_columns': [{'table': 'Wallets', 'column': 'WalletID', 'description': 'A unique identifier for each wallet.',
 'relevance_to_query': 'Unique identifier for the wallet entity; required for row identification and grouping.', 
 'sample_values': [7001, 7002], 'data_type': 'INTEGER'}, {'table': 'Wallets', 'column': 'UserID',
  'description': 'The unique identifier of the user who owns this wallet.', 
  'relevance_to_query': 'Necessary to identify and list the users associated with each wallet balance as requested in the subquestion.',
   'sample_values': [1, 2], 'data_type': 'INTEGER'},
    {'table': 'Wallets', 'column': 'Balance', 'description': 'The current monetary balance available in the wallet.', 
    'relevance_to_query': "Directly provides the 'current wallet balance' requested in the subquestion and the 'wallet balance' requested in the main question.",
     'sample_values': [450.75, 125.0], 'data_type': 'DECIMAL'}, {'table': 'Wallets', 'column': 'Currency', 'description': 'The currency in which the wallet balance is denominated.',
      'relevance_to_query': 'Essential for providing the unit context for the monetary balance values.', 
      'sample_values': ['USD', 'USD'], 'data_type': 'TEXT'}]}

for col_selec in trans_col['selected_columns']:
            print("col_selec:",col_selec)
        #     new_col = ["name of table:" + table_name] + col_selec
        #     inter.append(new_col)
        # final_col.extend(inter)
# def get_column_names(table_name: str) -> list[str]:
#     query = f"PRAGMA table_info({table_name});"
#     rows = run_sql_query(query)
#     return [row["name"] for row in rows]
# res=get_column_names("")
# table_list = [t.strip() for t in ['Users ', 'Products', 'Orders']]
# table_extract=[["List all orders with status 'delivered'", "Orders"]]
# table_names={}
# for table in table_list:
#     table_name=table
#     res=get_column_names(table_name)
#     table_names[table_name]=res
#     print(f"Columns for {table_name} are:{res}")


# def get_values(table,column):
#     query=f"SELECT DISTINCT {column} FROM {table}"
#     df=run_sql_query(query)
#     r=[c[column] for c in df]
#     return r


# def call_match(val):
#     final=[]
#     for i in val[1:]:
#         table=i[0]
#         column=i[1]
#         str_lst=[i.strip() for i in i[2].split(',')]

#         unq_col_val=get_values(table,column)
#         unq_col_val=[str(i) for i in unq_col_val]
#         print(unq_col_val)
#         for subval in str_lst:
#             best_match,score=get_best_match(subval,unq_col_val)
#             final.append({"table_name": table, "column_name":column, "filter_value":best_match})
#         return final

# filter_extract =['yes', ['Orders', 'Status', 'delivered']]
# res=call_match(filter_extract)
# # print(res)
# query1="""
# SELECT
#   o.OrderID,
#   o.Status,
#   s.OrderID AS Shipments_OrderID,
#   s.ShipmentID,
#   te.EventID,
#   te.ShipmentID AS TrackingEvents_ShipmentID,
#   te.StatusUpdate
# FROM Orders AS o
# INNER JOIN Shipments AS s
#   ON o.OrderID = s.OrderID
# INNER JOIN TrackingEvents AS te
#   ON s.ShipmentID = te.ShipmentID
# WHERE
#   o.Status = 'Delivered';"""

# response=run_sql_query(query1)
# print(response)