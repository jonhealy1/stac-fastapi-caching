from pyle38 import Tile38
import os
import json
import asyncio

tile38 = Tile38('redis://localhost:9851')

async def main():
    tile38 = Tile38(url="redis://localhost:9851", follower_url="redis://localhost:9851")

    user = {
        "first": "Tom",
        "last": "Anderson",
        "dict": {
            "job": "janitor"
        }
    }
    await tile38.jset('user', 'Tom', 'user', json.dumps(user))
    # await tile38.jset('user', 901, 'name.first', 'Anderson')
    response = await tile38.jget('user', 'Tom')
    response = json.loads(response.value)
    user = json.loads(response["user"])
    print(user)
    print(user["dict"]["job"])

    print(type(user))

asyncio.run(main())