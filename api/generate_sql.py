from agents.parent_agent import graph_main
from fastapi import HTTPException
from fastapi import APIRouter
from fastapi import logger
router = APIRouter()


@router.get('/generate')
async def get_sql_query(query:str):
    try:
        print("request received")
        if len(query)==0:
            raise HTTPException(status_code=442,detail=f"No query received:{query}")
        # res=graph_main.invoke({"user_query":query})
        # print(res['sql_query'])
        print(query)
        res="SELECT OrderID, Status, UserID, ProductID, OrderDate FROM Orders WHERE Status = 'Delivered"
        return {"success":True,"data":res}
        # return {"success":True,"data":res['sql_query']}
    except Exception as e:
        logger.error(f"Error in get_sql_query: {str(e)}")
        raise HTTPException(status_code=400,detail=f"Error:{str(e)}")
