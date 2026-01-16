import pandas as pd
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableMap, RunnableLambda
from sqlalchemy import create_engine
import tqdm
import time
import pickle
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

from info import execute_query,run_sql_query
load_dotenv()

import os
os.environ["GOOGLE_API_KEY"]
llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash")
# os.environ['OPENAI_API_KEY']
# llm=ChatOpenAI(
#     temperature=0.0,
#     model='xiaomi/mimo-v2-flash:free',
#     base_url='https://openrouter.ai/api/v1')

table_description = {
    "Users": "It contains details about users",
    "Products": "It contains all the details about the products",
    "Orders": "It contains details about orders",
    "Shipments": "It contains details about shipments like Estimated arrival of shipment and its tracking number",
    "Warehouses": "Contains details about warehouse which like which manager is controlling which warehouse",
    "TrackingEvents": "Contains details about events which has happened like which shipment was kept in which warehouse at what time and what is its status",
    "Wallets": "Contains details about wallet of user like how much balance does user has in his wallet",
    "Transactions": "Contains details about transactions which were made by user",
    "PaymentMethods": "It contains details about payment methods using which user can make a payment as the user might have multiple payment options",
    "Tickets": "It contains details about ticket details for a particular order or payment",
    "TicketMessages": "it contains details about message sent either by user or by some agent like I have received my mobile or like your parcel will arrive in 20 mins",
    "SatisfactionSurveys": "It contains detail about the survey that took place like what rating were given by user like 5 and what did he comment like best customer service "
}


template = ChatPromptTemplate.from_messages([
    ("system", """
You are an intelligent data annotator. Please annotate data as mentioned by human and give output without any verbose and without any additional explantion.
You will be given sql table description and sample columns from the sql table. The description that you generate will be given as input to text to sql automated system.
Output of project depends on how you generate description. Make sure your description has all possible nuances.

"""),

    ("human", '''

- Based on the column data, please generate description of entire table along with description for each column and sample values(1 or 2) for each column.
- While generating column descriptions, please look at sql table description given to you and try to include them in column description. 
- DONT write generic description like "It provides a comprehensive view of the order lifecycle from purchase to delivery". Just write description based on what you see in columns.

      
Context regarding the tables:
These tables represent OmniLife, an omni-retail system composed of four logically connected domains.
ShopCore manages users, products, and orders.
Each order belongs to one user and one product and is identified by OrderID.

ShipStream manages shipments and delivery tracking.
Each shipment is linked to exactly one order using OrderID.
Tracking events describe the movement of a shipment through warehouses.

PayGuard manages wallets, transactions, and payment methods.
Each wallet belongs to one user using UserID.
Transactions may reference an order using OrderID.

CareDesk manages customer support tickets and feedback.
Each ticket belongs to one user using UserID.
A ticket may reference an order or transaction using ReferenceID.

All systems are logically connected only through IDs (UserID, OrderID, TransactionID).
No table directly joins across domains.
Each domain should be queried independently and combined only at the application or agent level.
An order might have multiple items.
Each item might be fulfilled by a distinct seller.


Rules:
- Output MUST be valid JSON only
- No markdown
- No trailing commas
- No comments
- No explanations outside JSON
- Use double quotes for all strings
- The TOP-LEVEL JSON KEY MUST be exactly the SQL table name provided
- table_name MUST exactly match the provided SQL table name
- column_name MUST exactly match the provided SQL column name


What to generate:
1) A concise but precise table-level description based ONLY on the columns and data
2) A list of column annotations, one per column

For each column include:
- column_name
- description (based only on observed data)
- data_type (infer from values)
- sample_values (2 values, indicate implicitly that more exist)

Avoid:
- Generic descriptions
- Marketing language
- Abstract summaries

Output JSON schema (FOLLOW EXACTLY):
{{
  "<table_name>": {{
    "table_description": "string",
    "columns": {{
      "<column_name>": {{
        "description": "string",
        "data_type": "string",
        "sample_values": ["value1", "value2"]
      }}
    }}
  }}
}}


SQL table description:
{description}

Sample rows from the table:
{data_sample}     

Table names:
{tables}

column names:it is a dictionary which contains column names of particular table 
like 'Users': ['UserID', 'Name', 'Email', 'PremiumStatus'] so UserID, Name, Email, PremiumStatus are 
columns of User table so use them
{columns}
     ''')
])

# Fix the RunnableMap implementation
chain = (
    RunnableMap({
        "description": lambda x: x["description"],
        "data_sample": lambda x: x["data_sample"],
        "tables":lambda x:x['tables'],
        "columns":lambda x:x['columns']
    })
    | template
    | llm
    | StrOutputParser()
)
def read_sql(table):

# Query to get shuffled rows and limit to 5
    query = "SELECT * FROM {};".format(table)

    # Execute and load into DataFrame
    df_sample = execute_query(query)
    print("res:",df_sample)
    return df_sample


kb_final = {}

raw=execute_query("SELECT name FROM sqlite_master WHERE type='table'")

def get_column_names(table_name: str) -> list[str]:
    query = f"PRAGMA table_info({table_name});"
    rows = run_sql_query(query)
    return [row["name"] for row in rows]

table_names = []
table_columns={}
for row in raw:
    name = row[0]
    if isinstance(name, str) and name.isidentifier():
        columns=get_column_names(name)
        table_columns[name]=columns
        table_names.append(name)

for k,v in tqdm.tqdm(table_description.items()):
    d = read_sql(k)
    d_dict = str(d)
    response = chain.invoke({"description": v, "data_sample": d_dict,"tables":table_names,"columns":table_columns}).replace('```', '')
    print(response)
    print('====================================================')

    kb_final[k] = eval(response)
    time.sleep(5)

with open('kb4.pkl', 'wb') as f:
    pickle.dump(kb_final, f)