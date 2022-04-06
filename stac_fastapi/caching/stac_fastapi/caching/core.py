"""Item crud client."""
import json
import logging
from datetime import datetime as datetime_type
from datetime import timezone
from typing import List, Optional, Type, Union
from urllib.parse import urljoin

import attr
import stac_pydantic.api
from fastapi import HTTPException
from overrides import overrides
from pydantic import ValidationError
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes
from starlette.requests import Request

from stac_fastapi.caching import serializers
from stac_fastapi.caching.config import Tile38Settings
from stac_fastapi.caching.database_logic import DatabaseLogic
from stac_fastapi.caching.models.links import PagingLinks
from stac_fastapi.caching.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.caching.session import Session
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import AsyncBaseCoreClient, AsyncBaseTransactionsClient
from stac_fastapi.types.links import CollectionLinks
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection

logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CoreClient(AsyncBaseCoreClient):
    """Client for core endpoints defined by stac."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    item_serializer: Type[serializers.ItemSerializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.CollectionSerializer] = attr.ib(
        default=serializers.CollectionSerializer
    )
    database = DatabaseLogic()

    @overrides
    async def all_collections(self, **kwargs) -> Collections:
        """Read all collections from the database."""
        base_url = str(kwargs["request"].base_url)
        collection_list = await self.database.get_all_collections()
        collection_list = [
            self.collection_serializer.db_to_stac(c, base_url=base_url)
            for c in collection_list
        ]

        links = [
            {
                "rel": Relations.root.value,
                "type": MimeTypes.json,
                "href": base_url,
            },
            {
                "rel": Relations.parent.value,
                "type": MimeTypes.json,
                "href": base_url,
            },
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, "collections"),
            },
        ]

        return Collections(collections=collection_list, links=links)

    @overrides
    async def get_collection(self, collection_id: str, **kwargs) -> Collection:
        """Get collection by id."""
        base_url = str(kwargs["request"].base_url)
        collection = await self.database.find_collection(collection_id=collection_id)
        return self.collection_serializer.db_to_stac(collection, base_url)

    @overrides
    async def item_collection(
        self, collection_id: str, limit: int = 10, token: str = None, **kwargs
    ) -> ItemCollection:
        """Read an item collection from the database."""
        request: Request = kwargs["request"]
        base_url = str(kwargs["request"].base_url)

        items, maybe_count, next_token = await self.database.execute_search(
            search=self.database.apply_collections_filter(
                self.database.make_search(), [collection_id]
            ),
            limit=limit,
            token=token,
            sort=None,
        )

        items = [
            self.item_serializer.db_to_stac(item, base_url=base_url) for item in items
        ]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": len(items),
                "limit": limit,
            }
            if maybe_count is not None:
                context_obj["matched"] = maybe_count

        links = []
        if next_token:
            links = await PagingLinks(request=request, next=next_token).get_links()

        return ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            context=context_obj,
        )

    @overrides
    async def get_item(self, item_id: str, collection_id: str, **kwargs) -> Item:
        """Get item by item id, collection id."""
        base_url = str(kwargs["request"].base_url)
        item = await self.database.get_one_item(
            item_id=item_id, collection_id=collection_id
        )
        return self.item_serializer.db_to_stac(item, base_url)

    @staticmethod
    def _return_date(interval_str):
        intervals = interval_str.split("/")
        if len(intervals) == 1:
            datetime = intervals[0][0:19] + "Z"
            return {"eq": datetime}
        else:
            start_date = intervals[0]
            end_date = intervals[1]
            if ".." not in intervals:
                start_date = start_date[0:19] + "Z"
                end_date = end_date[0:19] + "Z"
            elif start_date != "..":
                start_date = start_date[0:19] + "Z"
                end_date = "2200-12-01T12:31:12Z"
            elif end_date != "..":
                start_date = "1900-10-01T00:00:00Z"
                end_date = end_date[0:19] + "Z"
            else:
                start_date = "1900-10-01T00:00:00Z"
                end_date = "2200-12-01T12:31:12Z"

            return {"lte": end_date, "gte": start_date}

    @overrides
    async def get_search(
        self,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime_type]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        **kwargs,
    ) -> ItemCollection:
        """GET search catalog."""
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": json.loads(query) if query else query,
        }
        if datetime:
            base_args["datetime"] = datetime
        if sortby:
            # https://github.com/radiantearth/stac-spec/tree/master/api-spec/extensions/sort#http-get-or-post-form
            sort_param = []
            for sort in sortby:
                sort_param.append(
                    {
                        "field": sort[1:],
                        "direction": "asc" if sort[0] == "+" else "desc",
                    }
                )
            base_args["sortby"] = sort_param

        # if fields:
        #     includes = set()
        #     excludes = set()
        #     for field in fields:
        #         if field[0] == "-":
        #             excludes.add(field[1:])
        #         elif field[0] == "+":
        #             includes.add(field[1:])
        #         else:
        #             includes.add(field)
        #     base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError:
            raise HTTPException(status_code=400, detail="Invalid parameters provided")
        resp = await self.post_search(search_request, request=kwargs["request"])

        return resp

    @overrides
    async def post_search(
        self, search_request: stac_pydantic.api.Search, **kwargs
    ) -> ItemCollection:
        """POST search catalog."""
        request: Request = kwargs["request"]
        base_url = str(request.base_url)

        # search = self.database.make_search()

        # if search_request.ids:
        #     search = self.database.apply_ids_filter(
        #         search=search, item_ids=search_request.ids
        #     )

        # if search_request.collections:
        #     search = self.database.apply_collections_filter(
        #         search=search, collection_ids=search_request.collections
        #     )

        # if search_request.datetime:
        #     datetime_search = self._return_date(search_request.datetime)
        #     search = self.database.apply_datetime_filter(
        #         search=search, datetime_search=datetime_search
        #     )

        items = []
        limit = search_request.limit
        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]
            items, count = await self.database.apply_bbox_filter(
                collection_id="test-collection", 
                bbox=search_request.bbox,
                limit=limit
            )

        #     search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if search_request.intersects:
            items, count = await self.database.apply_intersects_filter(
                intersects=search_request.intersects, limit=limit
            )

        # if search_request.query:
        #     for (field_name, expr) in search_request.query.items():
        #         field = "properties__" + field_name
        #         for (op, value) in expr.items():
        #             search = self.database.apply_stacql_filter(
        #                 search=search, op=op, field=field, value=value
        #             )

        # sort = None
        # if search_request.sortby:
        #     sort = self.database.populate_sort(search_request.sortby)

        # if search_request.limit:
        #     limit = search_request.limit

        # items, maybe_count, next_token = await self.database.execute_search(
        #     search=search_request,
        #     # limit=limit,
        #     # token=search_request.token,  # type: ignore
        #     # sort=sort,
        # )

        items = [
            self.item_serializer.db_to_stac(item, base_url=base_url) for item in items
        ]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": len(items),
                "limit": limit,
            }
            if count is not None:
                context_obj["matched"] = count

        links = []
        # if next_token:
        #     links = await PagingLinks(request=request, next=next_token).get_links()

        return ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            context=context_obj,
        )


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    database = DatabaseLogic()

    @overrides
    async def create_item(self, item: stac_types.Item, **kwargs) -> stac_types.Item:
        """Create item."""
        base_url = str(kwargs["request"].base_url)

        # If a feature collection is posted
        if item["type"] == "FeatureCollection":
            bulk_client = BulkTransactionsClient()
            processed_items = [
                bulk_client.preprocess_item(item, base_url) for item in item["features"]  # type: ignore
            ]
            await self.database.bulk_async(
                processed_items, refresh=kwargs.get("refresh", False)
            )

            return None  # type: ignore
        else:
            item = await self.database.prep_create_item(item=item, base_url=base_url)
            await self.database.create_item(item, refresh=kwargs.get("refresh", False))
            return item

    @overrides
    async def update_item(self, item: stac_types.Item, **kwargs) -> stac_types.Item:
        """Update item."""
        base_url = str(kwargs["request"].base_url)
        now = datetime_type.now(timezone.utc).isoformat().replace("+00:00", "Z")
        item["properties"]["updated"] = str(now)

        await self.database.check_collection_exists(collection_id=item["collection"])
        # todo: index instead of delete and create
        await self.delete_item(item_id=item["id"], collection_id=item["collection"])
        await self.create_item(item=item, **kwargs)

        return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def delete_item(
        self, item_id: str, collection_id: str, **kwargs
    ) -> stac_types.Item:
        """Delete item."""
        await self.database.delete_item(item_id=item_id, collection_id=collection_id)
        return None  # type: ignore

    @overrides
    async def create_collection(
        self, collection: stac_types.Collection, **kwargs
    ) -> stac_types.Collection:
        """Create collection."""
        base_url = str(kwargs["request"].base_url)
        collection_links = CollectionLinks(
            collection_id=collection["id"], base_url=base_url
        ).create_links()
        collection["links"] = collection_links
        await self.database.create_collection(collection=collection)

        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    async def update_collection(
        self, collection: stac_types.Collection, **kwargs
    ) -> stac_types.Collection:
        """Update collection."""
        base_url = str(kwargs["request"].base_url)

        await self.database.find_collection(collection_id=collection["id"])
        await self.delete_collection(collection["id"])
        await self.create_collection(collection, **kwargs)

        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    async def delete_collection(
        self, collection_id: str, **kwargs
    ) -> stac_types.Collection:
        """Delete collection."""
        await self.database.delete_collection(collection_id=collection_id)
        return None  # type: ignore


@attr.s
class BulkTransactionsClient(BaseBulkTransactionsClient):
    """Postgres bulk transactions."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    database = DatabaseLogic()

    def __attrs_post_init__(self):
        """Create es engine."""
        settings = Tile38Settings()
        self.client = settings.create_client

    def preprocess_item(self, item: stac_types.Item, base_url) -> stac_types.Item:
        """Preprocess items to match data model."""
        return self.database.sync_prep_create_item(item=item, base_url=base_url)

    @overrides
    def bulk_item_insert(
        self, items: Items, chunk_size: Optional[int] = None, **kwargs
    ) -> str:
        """Bulk item insertion using es."""
        request = kwargs.get("request")
        if request:
            base_url = str(request.base_url)
        else:
            base_url = ""

        processed_items = [
            self.preprocess_item(item, base_url) for item in items.items.values()
        ]

        self.database.bulk_sync(processed_items, refresh=kwargs.get("refresh", False))

        return f"Successfully added {len(processed_items)} Items."
