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
    response = await tile38.get('user', 'Tom').asObject()

    response = json.loads(response.object)
    user = json.loads(response["user"])
    print(user)
    print(user["dict"]["job"])

    print(type(user))
    # print(response)

    list = await tile38.scan('user').asObjects()
    print(list)
    print(list.count)
    user = json.loads(list.objects[1].object)
    print(json.loads(user["user"]))


asyncio.run(main())