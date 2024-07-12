from fastapi import FastAPI, HTTPException

import json

app = FastAPI()


@app.get("/pokemons")
async def get_pokemons():
    try:
        with open('./consumer/datas.json', 'r') as file:
            pokemons = json.load(file)
        return pokemons
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Data file not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding JSON")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
