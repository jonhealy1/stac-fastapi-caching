from pyle38 import Tile38
import os
import json
import asyncio

tile38 = Tile38('redis://localhost:9851')

async def main():
    tile38 = Tile38(url="redis://localhost:9851", follower_url="redis://localhost:9851")
    item = {
        "type": "Feature",
        "id": "test-item-57",
        "stac_version": "1.0.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json"
        ],
        "geometry": {
            "coordinates": [
            [
                [
                152.15052873427666,
                -33.82243006904891
                ],
                [
                150.1000346138806,
                -34.257132625788756
                ],
                [
                149.5776607193635,
                -32.514709769700254
                ],
                [
                151.6262528041627,
                -32.08081674221862
                ],
                [
                152.15052873427666,
                -33.82243006904891
                ]
            ]
            ],
            "type": "Polygon"
        },
        "properties": {
            "datetime": "2018-02-12T12:30:22Z",
            "landsat:scene_id": "LC82081612020043LGN00",
            "landsat:row": "161",
            "gsd": 15,
            "landsat:revision": "00",
            "view:sun_azimuth": -148.83296771,
            "instrument": "OLI_TIRS",
            "landsat:product_id": "LC08_L1GT_208161_20200212_20200212_01_RT",
            "eo:cloud_cover": 0,
            "landsat:tier": "RT",
            "landsat:processing_level": "L1GT",
            "landsat:column": "208",
            "platform": "landsat-8",
            "proj:epsg": 32756,
            "view:sun_elevation": -37.30791534,
            "view:off_nadir": 0,
            "height": 2500,
            "width": 2500
        },
        "bbox": [
            149.57574,
            -34.25796,
            152.15194,
            -32.07915
        ],
        "collection": "test-collection",
        "assets": {},
        "links": [
            {
            "href": "http://localhost:8081/collections/landsat-8-l1/items/LC82081612020043",
            "rel": "self",
            "type": "application/geo+json"
            },
            {
            "href": "http://localhost:8081/collections/landsat-8-l1",
            "rel": "parent",
            "type": "application/json"
            },
            {
            "href": "http://localhost:8081/collections/landsat-8-l1",
            "rel": "collection",
            "type": "application/json"
            },
            {
            "href": "http://localhost:8081/",
            "rel": "root",
            "type": "application/json"
            }
        ]
    }

    await tile38.set("test", item["id"]).object(item["geometry"]).exec()
    # await tile38.set("test", item["id"]).bounds(item["bbox"][0], item["bbox"][1], item["bbox"][2], item["bbox"][3]).exec()
    await tile38.jset("test", item["id"], 'item', json.dumps(item))
    bbox = [96.718129,-46.464978,178.632191,-1.445461]
    # objects = await tile38.intersects("test").bounds(bbox[0], bbox[1], bbox[2], bbox[3]).asObjects()
    objects = await tile38.intersects("test").object(item["geometry"]).asObjects()
    items = []
    print(objects.count)
    # print(objects)
    # print(objects.objects[0].id)
    for i in range(objects.count):
        item_result = objects.objects[i].object
        item_result["id"] = objects.objects[i].id
        items.append(json.loads(item_result["item"]))
        # item = json.loads(objects.objects[i].object)
        # items.append(json.loads(item["item"]))
    print(items)
    print(type(items[0]))
    

    # result = await tile38.get(item["collection"], item["id"]).asObject()
    # print(result.object)

    # print(type(result.object))

    # user = {
    #     "first": "Tom",
    #     "last": "Anderson",
    #     "dict": {
    #         "job": "janitor"
    #     }
    # }
    # await tile38.jset('user', 'Tom', 'user', json.dumps(user))
    # # await tile38.jset('user', 901, 'name.first', 'Anderson')
    # response = await tile38.get('user', 'Tom').asObject()

    # response = json.loads(response.object)
    # user = json.loads(response["user"])
    # print(user)
    # print(user["dict"]["job"])

    # print(type(user))
    # # print(response)

    # list = await tile38.scan('user').asObjects()
    # print(list)
    # print(list.count)
    # user = json.loads(list.objects[1].object)
    # print(json.loads(user["user"]))


asyncio.run(main())