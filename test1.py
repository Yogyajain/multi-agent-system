from knowledge_base.info import run_sql_query
import pickle
from agents.fuzzy import call_match,get_best_match
q="select * from orders"
resp=run_sql_query(q)
print(resp)

# with open('kb4.pkl', 'rb') as f:
#     loaded_dict = pickle.load(f)
# def get_column_names(table_name: str) -> list[str]:
#     query = f"PRAGMA table_info({table_name});"
#     rows = run_sql_query(query)
#     return [row["name"] for row in rows]
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