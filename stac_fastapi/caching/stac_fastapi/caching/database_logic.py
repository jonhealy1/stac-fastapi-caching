"""Database logic."""
# import asyncio
import json
import logging

# from http import client
from typing import Any, Dict, List, Type, Union

import attr
import pyle38

from stac_fastapi.caching import serializers
from stac_fastapi.caching.config import AsyncTile38Settings
from stac_fastapi.caching.config import Tile38Settings as SyncTile38Settings
from stac_fastapi.types.errors import ConflictError, NotFoundError
from stac_fastapi.types.stac import Collection, Item

# from base64 import urlsafe_b64decode, urlsafe_b64encode


# from geojson_pydantic.geometries import (
#     GeometryCollection,
#     LineString,
#     MultiLineString,
#     MultiPoint,
#     MultiPolygon,
#     Point,
#     Polygon,
# )


logger = logging.getLogger(__name__)

NumType = Union[float, int]

ITEMS_INDEX = "stac_items"
COLLECTIONS_INDEX = "stac_collections"

DEFAULT_SORT = {
    "properties.datetime": {"order": "desc"},
    "id": {"order": "desc"},
    "collection": {"order": "desc"},
}


def bbox2polygon(b0, b1, b2, b3):
    """Transform bbox to polygon."""
    return [[[b0, b1], [b2, b1], [b2, b3], [b0, b3], [b0, b1]]]


def mk_item_id(item_id: str, collection_id: str):
    """Make the Tile38 document _id value from the Item id and collection."""
    return f"{item_id}|{collection_id}"


@attr.s
class DatabaseLogic:
    """Database logic."""

    client = AsyncTile38Settings().create_client
    sync_client = SyncTile38Settings().create_client

    item_serializer: Type[serializers.ItemSerializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.CollectionSerializer] = attr.ib(
        default=serializers.CollectionSerializer
    )

    """CORE LOGIC"""

    async def get_all_collections(self) -> List[Dict[str, Any]]:
        """Database logic to retrieve a list of all collections."""
        # https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/65
        # collections should be paginated, but at least return more than the default 10 for now
        objects = await self.client.scan("collections").asObjects()
        collections = []
        for i in range(objects.count):
            collection = json.loads(objects.objects[i].object)
            collections.append(json.loads(collection["collection"]))
        return collections

    async def get_item_collection(self) -> List[Dict[str, Any]]:
        """Database logic to retrieve a list of all items in a collection."""
        objects = await self.client.scan("stac_items").asObjects()
        items = []
        for i in range(objects.count):
            # item = json.loads(objects.objects[i].object)
            item = objects.objects[i].object
            items.append(json.loads(item["item"]))
        return items, 10, None

    async def get_one_item(self, collection_id: str, item_id: str) -> Dict:
        """Database logic to retrieve a single item."""
        db_id = mk_item_id(item_id=item_id, collection_id=collection_id)
        try:
            response = await self.client.jget("stac_items", db_id)
        except pyle38.errors.Tile38IdNotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist in Collection {collection_id}"
            )
        except pyle38.errors.Tile38KeyNotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist in Collection {collection_id}"
            )
        response = json.loads(response.value)
        item = json.loads(response["item"])
        return item

    @staticmethod
    def make_search():
        """Database logic to create a Search instance."""
        pass
        # return Search().sort(*DEFAULT_SORT)

    # @staticmethod
    # def apply_ids_filter(search: Search, item_ids: List[str]):
    #     """Database logic to search a list of STAC item ids."""
    #     return search.filter("terms", id=item_ids)

    @staticmethod
    def apply_collections_filter(search: dict, collection_ids: List[str]):
        """Database logic to search a list of STAC collection ids."""
        pass
        # return search.filter("terms", collection=collection_ids)

    # @staticmethod
    # def apply_datetime_filter(search: Search, datetime_search):
    #     """Database logic to search datetime field."""
    #     if "eq" in datetime_search:
    #         search = search.filter(
    #             "term", **{"properties__datetime": datetime_search["eq"]}
    #         )
    #     else:
    #         search = search.filter(
    #             "range", properties__datetime={"lte": datetime_search["lte"]}
    #         )
    #         search = search.filter(
    #             "range", properties__datetime={"gte": datetime_search["gte"]}
    #         )
    #     return search

    # @staticmethod
    async def apply_bbox_filter(self, collection_id: str, bbox: List, limit: int):
        # def apply_bbox_filter(search: Search, bbox: List):
        """Database logic to search on bounding box."""
        # objects = await self.client.intersects("test").bounds(bbox[0],bbox[1],bbox[2],bbox[3]).asObjects()
        geom = {
            "type": "Polygon",
            "coordinates": bbox2polygon(bbox[0], bbox[1], bbox[2], bbox[3]),
        }
        objects = await self.client.intersects("stac_items").object(geom).asObjects()
        count = objects.count
        items = []
        if count < limit:
            limit = count
        for i in range(limit):
            item_result = objects.objects[i].object
            item_result["id"] = objects.objects[i].id
            items.append(json.loads(item_result["item"]))

        return items, count

    # @staticmethod
    async def apply_intersects_filter(self, intersects: dict, limit: int):
        #     self,
        #     search: dict
        #     intersects: Union[
        #         Point,
        #         MultiPoint,
        #         LineString,
        #         MultiLineString,
        #         Polygon,
        #         MultiPolygon,
        #         GeometryCollection,
        #     ],
        # ):
        """Database logic to search a geojson object."""
        items = []
        if intersects.type == "Point":
            new_intersects = {}
            point = intersects.coordinates
            new_intersects["type"] = "Polygon"
            new_intersects["coordinates"] = bbox2polygon(
                float(point[0]),
                float(point[1]),
                float(point[0]) + 0.001,
                float(point[1]) + 0.001,
            )
            objects = (
                await self.client.intersects("stac_items")
                .object(new_intersects)
                .asObjects()
            )
        elif intersects.type == "Polygon":
            objects = (
                await self.client.intersects("stac_items")
                .object(intersects)
                .asObjects()
            )
        else:
            return items, 0
        count = objects.count
        if count < limit:
            limit = count
        for i in range(limit):
            item_result = objects.objects[i].object
            item_result["id"] = objects.objects[i].id
            items.append(json.loads(item_result["item"]))

        return items, count

    # @staticmethod
    # def apply_stacql_filter(search: Search, op: str, field: str, value: float):
    #     """Database logic to perform query for search endpoint."""
    #     if op != "eq":
    #         key_filter = {field: {f"{op}": value}}
    #         search = search.filter(Q("range", **key_filter))
    #     else:
    #         search = search.filter("term", **{field: value})

    #     return search

    # @staticmethod
    # def populate_sort(sortby: List) -> Optional[Dict[str, Dict[str, str]]]:
    #     """Database logic to sort search instance."""
    #     if sortby:
    #         return {s.field: {"order": s.direction} for s in sortby}
    #     else:
    #         return None
    # async def execute_search(self, search: dict):
    #     items, count = await self.apply_bbox_filter(collection_id="test-collection", bbox=search.bbox)
    #     return items, count, None

    # async def execute_search(
    #     self,
    #     search: Search,
    #     limit: int,
    #     token: Optional[str],
    #     sort: Optional[Dict[str, Dict[str, str]]],
    # ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
    #     """Database logic to execute search with limit."""
    #     search_after = None
    #     if token:
    #         search_after = urlsafe_b64decode(token.encode()).decode().split(",")

    #     query = search.query.to_dict() if search.query else None

    #     search_task = asyncio.create_task(
    #         self.client.search(
    #             index=ITEMS_INDEX,
    #             query=query,
    #             sort=sort or DEFAULT_SORT,
    #             search_after=search_after,
    #             size=limit,
    #         )
    #     )

    #     count_task = asyncio.create_task(
    #         self.client.count(index=ITEMS_INDEX, body=search.to_dict(count=True))
    #     )

    #     es_response = await search_task

    #     hits = es_response["hits"]["hits"]
    #     items = (hit["_source"] for hit in hits)

    #     next_token = None
    #     if hits and (sort_array := hits[-1].get("sort")):
    #         next_token = urlsafe_b64encode(
    #             ",".join([str(x) for x in sort_array]).encode()
    #         ).decode()

    #     # (1) count should not block returning results, so don't wait for it to be done
    #     # (2) don't cancel the task so that it will populate the ES cache for subsequent counts
    #     maybe_count = None
    #     if count_task.done():
    #         try:
    #             maybe_count = count_task.result().get("count")
    #         except Exception as e:  # type: ignore
    #             logger.error(f"Count task failed: {e}")

    #     return items, maybe_count, next_token

    """ TRANSACTION LOGIC """

    async def check_collection_exists(self, collection_id: str):
        """Database logic to check if a collection exists."""
        try:
            await self.client.jget("collections", collection_id)
        except pyle38.errors.Tile38IdNotFoundError:
            raise NotFoundError(f"Collection {collection_id} does not exist")
        except pyle38.errors.Tile38KeyNotFoundError:
            raise NotFoundError(f"Collection {collection_id} does not exist")

    async def prep_create_item(self, item: Item, base_url: str) -> Item:
        """Database logic for prepping an item for insertion."""
        await self.check_collection_exists(collection_id=item["collection"])

        db_id = mk_item_id(item_id=item["id"], collection_id=item["collection"])

        try:
            await self.client.jget("stac_items", db_id)
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )
        except pyle38.errors.Tile38IdNotFoundError:
            pass
        except pyle38.errors.Tile38KeyNotFoundError:
            pass

        return self.item_serializer.stac_to_db(item, base_url)

    # def sync_prep_create_item(self, item: Item, base_url: str) -> Item:
    #     """Database logic for prepping an item for insertion."""
    # collection_id = item["collection"]
    # if not self.sync_client.exists(index=COLLECTIONS_INDEX, id=collection_id):
    #     raise NotFoundError(f"Collection {collection_id} does not exist")

    #     if self.sync_client.exists(
    #         index=ITEMS_INDEX, id=mk_item_id(item["id"], item["collection"])
    #     ):
    #         raise ConflictError(
    #             f"Item {item['id']} in collection {item['collection']} already exists"
    #         )

    #     return self.item_serializer.stac_to_db(item, base_url)

    async def create_item(self, item: Item, refresh: bool = False):
        """Database logic for creating one item."""
        # todo: check if collection exists, but cache
        db_id = mk_item_id(item_id=item["id"], collection_id=item["collection"])

        try:
            await self.client.jget("stac_items", db_id)
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )
        except pyle38.errors.Tile38IdNotFoundError:
            pass
        except pyle38.errors.Tile38KeyNotFoundError:
            pass

        await self.client.set("stac_items", db_id).object(item["geometry"]).exec()
        await self.client.jset("stac_items", db_id, "item", json.dumps(item))

    async def delete_item(
        self, item_id: str, collection_id: str, refresh: bool = False
    ):
        """Database logic for deleting one item."""
        db_id = mk_item_id(item_id=item_id, collection_id=collection_id)
        try:
            await self.client.expire("stac_items", db_id, 0.1)
        except pyle38.errors.Tile38IdNotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} not found"
            )

    async def create_collection(self, collection: Collection, refresh: bool = False):
        """Database logic for creating one collection."""
        try:
            await self.client.jget("collections", collection["id"])
            raise ConflictError(f"Collection {collection['id']} already exists")
        except pyle38.errors.Tile38IdNotFoundError:
            pass
        except pyle38.errors.Tile38KeyNotFoundError:
            pass

        await self.client.jset(
            "collections", collection["id"], "collection", json.dumps(collection)
        )

    async def find_collection(self, collection_id: str) -> Collection:
        """Database logic to find and return a collection."""
        try:
            response = await self.client.jget("collections", collection_id)
        except pyle38.errors.Tile38IdNotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")
        except pyle38.errors.Tile38KeyNotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")
        response = json.loads(response.value)
        collection = json.loads(response["collection"])
        return collection

    async def delete_collection(self, collection_id: str, refresh: bool = False):
        """Database logic for deleting one collection."""
        await self.find_collection(collection_id=collection_id)
        await self.client.expire("collections", collection_id, 0.1)

    # async def bulk_async(self, processed_items, refresh: bool = False):
    #     """Database logic for async bulk item insertion."""
    #     await asyncio.get_event_loop().run_in_executor(
    #         None,
    #         lambda: helpers.bulk(
    #             self.sync_client, self._mk_actions(processed_items), refresh=refresh
    #         ),
    #     )

    # def bulk_sync(self, processed_items, refresh: bool = False):
    #     """Database logic for sync bulk item insertion."""
    #     helpers.bulk(
    #         self.sync_client, self._mk_actions(processed_items), refresh=refresh
    #     )

    # @staticmethod
    # def _mk_actions(processed_items):
    #     return [
    #         {
    #             "_index": ITEMS_INDEX,
    #             "_id": mk_item_id(item["id"], item["collection"]),
    #             "_source": item,
    #         }
    #         for item in processed_items
    #     ]

    # DANGER
    async def delete_items(self) -> None:
        """Danger. this is only for tests."""
        await self.client.drop(ITEMS_INDEX)

    # DANGER
    async def delete_collections(self) -> None:
        """Danger. this is only for tests."""
        await self.client.drop(COLLECTIONS_INDEX)
