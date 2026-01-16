from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableMap, RunnableLambda
import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
import os

load_dotenv()
os.environ["GOOGLE_API_KEY"]
llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash")
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

