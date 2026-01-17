from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableMap, RunnableLambda
import pickle
import re
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
import os

load_dotenv()

os.environ["GOOGLE_API_KEY"]
llm = ChatGoogleGenerativeAI(model="gemini-3-flash-preview")


template = ChatPromptTemplate.from_messages([
    ("system", """
You are an intelligent router in text to sql system that understands the user question and 
determines which agents might have answer to the question based on agent description. Multiple agents might answer a given user question. OUTPUT SHOULD BE IN FORM OF LIST OF strings.
Dont give any explanation or any other verbose in the output.
"""),

    ("human", '''
Below are descriptions of different agents.
ShopCore agent : It contains all the details about users like user identifiers and user information, products like product identifier, product category, description, dimensions and product details, and orders like order identifier, products in an order, number of items, price of order, order time, and order status
ShipStream agent : It contains details about shipments like shipment identifier, tracking number, estimated arrival time, shipment status, warehouses like warehouse identifier, warehouse manager, warehouse location, and tracking events like which shipment was kept in which warehouse at what time and its status
PayGuard agent : It contains details about wallets like wallet identifier, user wallet balance, transactions like transaction identifier, transaction amount, transaction time, transaction status, and payment methods like payment method identifier, payment method type, and payment options available to users
CareDesk agent : It contains details about tickets like ticket identifier, ticket status, ticket creation time, ticket reference to orders or transactions, ticket messages like message content, message sender, message time, and satisfaction surveys like survey identifier, user rating, user comments, and feedback


STEP BY STEP TABLE SELECTION PROCESS:
- Split the question into different subquestions.
- For each subquestion, very carefully go through each and every AGENT description, think which agent might have answer to this subquestion.
- At the end collect all the agents that you thought can answer the whole question in form of list of strings
- For a give question, if ShopCore and ShipStream agents can answer question, give output like below without any verbose.
['ShopCore', 'ShipStream']
- If only ShopCore can answer a question , give output like below with one agent in list
['ShopCore']
- For a give question, if ShopCore and PayGuard and CareDesk agents can answer question, give output like below without any verbose.
['ShopCore', 'PayGuard', 'CareDesk']
     
User question:
{question}

     ''')
])

# Fix the RunnableMap implementation
chain = (
    RunnableMap({
        "question": lambda x: x["question"]
    })
    | template 
    | llm 
    | StrOutputParser()
)

def agent_2(q):
    response = chain.invoke({"question": q}).replace('```', '')
    return response


template_subquestion = ChatPromptTemplate.from_messages([
    ("system", """
You are an intelligent subquestion generator that extracts subquestions based on human instruction and the CONTEXT provided. You are part of a Text-to-SQL agent.
"""),

    ("human", '''
CONTEXT:
This dataset pertains to Olist, the largest department store on Brazilian marketplaces.
When a customer purchases a product from the Olist store (from a specific seller and location), the seller is notified to fulfill the order.
Once the customer receives the product or the estimated delivery date passes, the customer receives a satisfaction survey via email to rate their purchase experience and leave comments.

You are given:
- A user question
- A list of table names with descriptions

Instructions:
Think like a Text-to-SQL agent. When selecting tables, carefully consider whether multiple tables need to be joined. Only select the tables necessary to answer the user question.
*** A table might not answer a subquestion, but adding it might act as a link with another table selected by different agent. Think in this way while selecting a table. If selected table has all information, ignore other tables.


Your task:
1. Break the user question into minimal, specific subquestions that represent distinct parts of the information being requested.
2. For each subquestion, identify a **single table** whose **description** clearly indicates it contains the needed information.
3. **Ignore any subquestion that cannot be answered using the provided tables.**
4. **Only include subquestions that directly contribute to answering the main user question.**
5. If a subquestion can be answered using multiple tables, intelligently choose the single most appropriate table based on the description.
6. Be highly specific and avoid redundant or irrelevant subquestions. For example, if the number of orders is asked, only use order IDs—no other order details are needed.

Additional Guidelines:
- Fully understand the CONTEXT above before attempting subquestion generation. This is crucial to identifying relevant data.
- You are NOT answering the question itself.
- You are NOT responsible for whether the entire question is answerable from the available data.
- Your ONLY job is to check whether a specific subpart of the question can be answered from a table based on its description.
- If multiple subquestions map to the same table, group them into a single list entry like club multiple subquestions into 1 single question.
- A table might not answer a subquestion, but adding it might act as a link with another table selected by different agent that helps answering user question. Think in this way while selecting a table. If selected table has all information, ignore other tables.
- STRICTLY exclude subquestions that no table can answer.
- Length of each sublist should be exactly 2 as per below output format.

Output format:
Return a list of lists in the following format. Ensure all strings use double quotes. Length of each sublist should be exactly 2. :
[["subquestion1", "table name 1"], ["subquestion2", "table name 2"]]

If multiple subquestions map to the same table:
[["subquestion1", "subquestion2", "table name"]]

If only one valid subquestion:
[["subquestion1", "table name"]]

If no valid subquestions:
[[]]

---

Examples

Question: "Give me the list of customers who have bought more than 5 products in the last month using UPI. Also, list the product categories they purchased."

HOW TO THINK STEP BY STEP:
- Understand the CONTEXT and business process.
- “List of customers” → Check if any table tracks customers → Yes, "customer" table.  Check if any table is needed that has link with customer identifier and helps in answering user question
- “More than 5 products bought” → Check for product identifiers. → Yes, "product" table.  Check if any table is needed that has link with product identifier and helps in answering user question
- “Product categories purchased” → Check for product category info → Yes, "product" table.  Check if any table is needed that has link with product identifier and helps in answering user question
- “Using UPI” → Is there a table with payment method? → No, ignore this subquestion. 
- “Product table” answers two subquestions → Group both.
- Do a final check if i missed anything 

Output guidelines:
- No markdown
- No code fences
- No extra keys
- No explanations

Output: 
[
  ["List of customers", "customer"],
  ["Total products bought per customer", "Product categories purchased", "product"]
]

---

Table List:
{tables}

User question:
{user_query}
''')
])



chain_subquestion = (
    RunnableMap({
        "tables": lambda x: x["tables"],
        "user_query": lambda x: x["user_query"]
    })
    | template_subquestion
    | llm
    | StrOutputParser()
)


template_column = ChatPromptTemplate.from_messages([
    ("system", """
You are an intelligent data column selector that chooses the most relevant columns from a list of available column descriptions to help answer a subquestion ONLY.
Your selections will be used by a SQL generation agent, so choose **only those columns** that will help write the correct SQL query for a subquestion based on main question.

Act like you're preparing the exact inputs required to build the SQL logic. Also, look at main user question before selecting columns.
BUT main PRIORITY IS TO SELECT columns for subquestion.
"""),

    ("human", '''
     
HOW TO THINK STEP BY STEP:
- For each subquestion mentioned in subquestion below, think if <column1> in Column list might help in answering the question based on column description below. If no, check if this column can be used to answer any part of main question below.
    subquestion, main question: If column1 is used to answer any of these based on column description? If yes, then select that column
    subquestion, main question: If column2 is used to answer any of these based on column description? If yes, then select that column
    and so on.. for all columns

- There can be **critical dependencies between different columns** to answer a subquestion. THIS IS MANDATORY STEP TO THINK LIKE THIS WHEREVER NECESSARY.
  For example, a total value or aggregated metric may require combining:
    - multiple values from repeated rows (e.g., payments, items, events)
    - additional grouping or identifier columns
    - columns that explain how a value is split or repeated (e.g., installments, splits, quantities)

  In such cases, **select all contributing columns together**. Never assume a single column alone is sufficient for aggregated results.

- Include any supporting columns that help define or group the main entity (e.g., order_id if the question asks for order-level info).
- Only after processing the subquestion completely, look at main question to see if it adds any more relevant columns.

RULES:
1. ALWAYS include any **unique identifiers** related to the entity being queried (e.g., order_id, product_id, customer_id).
2. **NEVER select** the `customer_unique_id` column — it must always be ignored.
3. ONLY before generating final list of columns, check main question if any other column helps in answering it.
4. When a metric or value depends on multiple rows, parts, quantities, or repeated fields, you must include all columns required to fully calculate or group that metric.
5. While selecting columns, describe how each selected column **contributes to solving the subquestion**, and include full column description from column list.
6. Output must be in the format specified below. Length of each sublist should be exactly 2.

**Output Format:**
Output should look like below list of lists format for sure.. This is mandatory. Make sure to say there are many other values in that column apart from sample values. Length of each sublist should be exactly 2.
[["<column name 1>", "<Add column description as per column list below + What part of question this column answers something like this column tells about.... sample values:<> and add so on...>"], ["<column name 2>", "<Add column description as per column list below + What part of question this column answers like this column tells about.... .sample values:<> and so on in sample values>"]]

Example: To know total value of an order, order might have multiple items. We might need to consider number of items * price of each item to know cost of order.
Here, to calculate cost of order, we have considered number of items and price of each item. To solve 1 aspect of the question we required both columns.
Carefully look at such scenarios based on column descriptions. Sometimes, there might not be such cases. Be careful and consider descriptions of different column.

---

Column list:
{columns}
     
subquestion:
{query}
     
Main question:
{main_question}
    ''')
])



chain_column_extractor = (
    RunnableMap({
        "columns": lambda x: x["columns"],
        "query": lambda x: x["query"],
        "main_question": lambda x: x["main_question"]
    })
    | template_column
    | llm
    | StrOutputParser()
)


template_filter_check = ChatPromptTemplate.from_messages([
    ("system", """
You are an expert assistant designed to help a text-to-SQL agent determine whether filters (i.e., WHERE clauses) are required for answering a user's natural language question using a SQL query on a database.

Your job is to:
1. Carefully analyze the user question and identify if any filtering condition is implied (e.g., city = 'Campinas', date range, payment type = 'credit_card', etc.).
2. Use the provided list of tables and columns (with sample values) to identify which specific string datatype **columns** would be involved in such filtering.
     
3. Determine whether a **filter is needed** ONLY for string datatype columns:
    - If **yes**, return a list in the format:
      ["yes", ["<table>", "<column>", "<filter values exactly as stated in the user question>"], ["<table2>", "<column2>", "<filter values exactly as stated in the user question>"], ...]
    - If **no filter is needed**, return: ["no"]
4. For the third item in each filter entry, suggest value(s) **exactly as stated in the user query**, even if they are different from the sample values.
   - If user says "New York" and the column has "SF" it means in actual columns values are in abbrevation, so output "NY". Suggest based on user question and sample values.
   - If user says "credit card or boleto", output "credit card, boleto"
5. Only include columns in the output that help **narrow down** the dataset, such as city, state, payment method etc.
6. For float or integer or DATE datatype columns just give ["no"] as output.  For date kind of columns give output as ["no"]
7. Output should be STRICTLY in form of list.

⚠️ Be careful not to include aggregation or grouping columns like `customer_id`, `order_id`, or `product_id` unless they are being **explicitly** filtered in the question.

Example outputs:
[
  "yes",["Tickets", "reference_type", "order"],["SatisfactionSurveys", "rating", "1, 2"]]
["no"]
    """),

    ("human", '''
Given a user query and the available list of tables and column names (with sample values), decide if the SQL query to answer this question requires filters.

Only return a list in the exact format described:
- "yes" if filtering is needed, followed by the relevant table-column-filter entries.
- "no" if the question can be answered using full-table aggregates or joins without conditions. For float ot integer or date datatype columns also just give ["no"] as output.
- Make sure that output should be strictly in terms of list or list of lists. Make sure strings within these lists are properly closed by ".

Here is the user question:
{query}

And here is the list of available tables and columns (with sample values):
{columns}
''')
])



chain_filter_extractor = (
    RunnableMap({
        "columns": lambda x: x["columns"],
        "query": lambda x: x["query"]
    })
    | template_filter_check
    | llm
    | StrOutputParser()
)


template_sql_query = ChatPromptTemplate.from_messages([
("system", """
You are a highly strict MySQL query generator.

Your job is to generate ONLY syntactically correct, executable MySQL queries using the provided schema.

CRITICAL RULES:
1. You MUST use every column listed under "Relevant tables and columns".
2. You MUST NOT invent new columns or tables.
3. Every column must appear either:
   - In SELECT
   - In JOIN condition
   - In WHERE
   - In GROUP BY
   - In ORDER BY
4. If aggregation is used, ALL non-aggregated columns must appear in GROUP BY.
5. NEVER skip a provided column even if it looks irrelevant.
6. Use table aliases, but NEVER use reserved keywords as aliases (e.g., or, and, as, select, where).
7. Only generate valid MySQL syntax (no PostgreSQL/SQL Server features).
8. Use CTE (WITH) only if absolutely needed for grouped filtering.
9. Apply filters ONLY if they are listed in Applicable filters.
10. Output ONLY the final SQL query. No explanations.

Your output must be 100% executable in MySQL.
"""),

("human", """
You are given:

• A user question
• A list of mandatory tables and columns selected by a prior schema reasoning agent
• Optional filters that MUST be applied if present

STRICT INSTRUCTIONS:

STEP 1 — COLUMN USAGE
- Every column listed is mandatory.
- Ensure each column appears in SELECT or contributes to JOIN/logic.
- If a column represents metadata, include it in SELECT.

STEP 2 — TABLE RELATIONSHIPS
- Infer JOIN keys only from provided columns.
- Never hallucinate keys.
- Use INNER JOIN unless question clearly requires otherwise.

STEP 3 — FILTER APPLICATION
- Apply filters exactly as given.
- Respect column types.
- For NULL and "none" values, handle explicitly using:
    column IS NOT NULL
    column <> 'none'

STEP 4 — AGGREGATION SAFETY
- If GROUP BY exists:
    include all non-aggregated columns.
- If HAVING is needed, wrap aggregation in subquery or CTE.

STEP 5 — VALIDATION BEFORE OUTPUT
Ensure:
✔ All columns used
✔ No invented schema
✔ Valid MySQL syntax
✔ Proper GROUP BY usage
✔ Safe aliases
✔ Filters correctly applied

User question:
{query}

Relevant tables and columns:
{columns}

Applicable filters:
{filters}
""")
])



chain_query_extractor = (
    RunnableMap({
        "columns": lambda x: x["columns"],
        "query": lambda x: x["query"],
        "filters": lambda x: x["filters"]
    })
    | template_sql_query
    | llm
    | StrOutputParser()
)

template_validation = ChatPromptTemplate.from_messages([
("system", """
You are a strict MySQL query validator and repair agent.

Your job is to verify that the provided SQL query is:
• Syntactically valid in MySQL
• Logically aligned with the user's question
• Fully compliant with the mandatory schema
• Free from invalid aliases or SQL errors

You must ONLY correct errors. You must NOT redesign the query unnecessarily.

CRITICAL RULES:
1. Every column listed in "Relevant Tables and Columns" is mandatory.
   - They must appear in SELECT or be meaningfully used in JOIN / WHERE / GROUP BY.
   - You are NOT allowed to remove them from the query.
2. Never introduce new tables or columns.
3. Ensure all JOINs are valid and based only on provided columns.
4. Fix alias issues:
   - Replace aliases that use reserved words (or, and, as, select, where, group, order, etc.).
5. Enforce MySQL aggregation rules:
   - Every non-aggregated SELECT column must appear in GROUP BY.
6. If HAVING or grouped filtering is logically incorrect, rewrite using a subquery.
7. Filters must exactly match "Applicable Filters".
8. Output ONLY the final corrected SQL query.
9. If the query is already fully valid, return it unchanged.
"""),

("human", """
Validate the SQL query using strict MySQL correctness.

Checklist before returning:
✔ All mandatory columns present
✔ No hallucinated schema
✔ Valid JOIN logic
✔ Valid GROUP BY usage
✔ No reserved keyword aliases
✔ Filters correctly applied
✔ Query executable in MySQL

User Question:
{query}

Relevant Tables and Columns:
{columns}

Applicable Filters:
{filters}

SQL Query to Validate:
{sql_query}
""")
])

chain_query_validator = (
    RunnableMap({
        "columns": lambda x: x["columns"],
        "query": lambda x: x["query"],
        "filters": lambda x: x["filters"],
        'sql_query': lambda x: x["sql_query"],
    })
    | template_validation
    | llm
    | StrOutputParser()
)