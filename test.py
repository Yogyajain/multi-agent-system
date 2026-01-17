from parent_agent import graph_main

q1="""
I ordered a 'Gaming Monitor' last week, but it hasn't arrived. 
I opened a ticket about this yesterday. Can you tell me where the package is right now and if my ticket has been assigned 
to an agent?"
"""

q="List all delivered orders"
f=graph_main.invoke({"user_query":q})
print(f"Yogi's agent:{f['sql_query']}")