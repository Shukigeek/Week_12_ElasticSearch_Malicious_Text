import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from system_manager.manager import Manager

app = FastAPI()
m = Manager()


@app.get("/antisemitic-with-weapon")
def get_antisemitic_with_weapon():
    try:
        docs = m.docs_antisemitic_with_some_weapon()
        if not docs:
            return JSONResponse(content={"message": "Data not processed or no matching documents."}, status_code=404)
        return docs
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/documents-with-two-weapons")
def get_documents_with_two_weapons():
    try:
        docs = m.docs_with_two_weapons()
        if not docs:
            return JSONResponse(content={"message": "Data not processed or no matching documents."}, status_code=404)
        return docs
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
