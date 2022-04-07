import copy
import uuid
from datetime import datetime, timedelta

import pytest

from ..conftest import MockRequest, create_collection, create_item

ROUTES = {
    "GET /_mgmt/ping",
    "GET /docs/oauth2-redirect",
    "HEAD /docs/oauth2-redirect",
    "GET /",
    "GET /conformance",
    "GET /api",
    "GET /api.html",
    "HEAD /api",
    "HEAD /api.html",
    "GET /collections",
    "GET /collections/{collection_id}",
    "GET /collections/{collection_id}/items",
    "GET /collections/{collection_id}/items/{item_id}",
    "GET /search",
    "POST /search",
    "DELETE /collections/{collection_id}",
    "DELETE /collections/{collection_id}/items/{item_id}",
    "POST /collections",
    "POST /collections/{collection_id}/items",
    "PUT /collections",
    "PUT /collections/{collection_id}/items",
}


async def test_post_search_content_type(app_client, ctx):
    params = {"limit": 1}
    resp = await app_client.post("/search", json=params)
    assert resp.headers["content-type"] == "application/geo+json"


async def test_get_search_content_type(app_client, ctx):
    resp = await app_client.get("/search")
    assert resp.headers["content-type"] == "application/geo+json"


async def test_api_headers(app_client):
    resp = await app_client.get("/api")
    assert (
        resp.headers["content-type"] == "application/vnd.oai.openapi+json;version=3.0"
    )
    assert resp.status_code == 200


async def test_router(app):
    api_routes = set([f"{list(route.methods)[0]} {route.path}" for route in app.routes])
    assert len(api_routes - ROUTES) == 0


async def test_app_transaction_extension(app_client, ctx):
    item = copy.deepcopy(ctx.item)
    item["id"] = str(uuid.uuid4())
    resp = await app_client.post(f"/collections/{item['collection']}/items", json=item)
    assert resp.status_code == 200

    await app_client.delete(f"/collections/{item['collection']}/items/{item['id']}")


async def test_app_search_response(app_client, ctx):
    resp = await app_client.get("/search", params={"ids": ["test-item"]})
    assert resp.status_code == 200
    resp_json = resp.json()

    assert resp_json.get("type") == "FeatureCollection"
    # stac_version and stac_extensions were removed in v1.0.0-beta.3
    assert resp_json.get("stac_version") is None
    assert resp_json.get("stac_extensions") is None


async def test_app_context_extension(app_client, ctx, txn_client):
    test_item = ctx.item
    test_item["id"] = "test-item-2"
    test_item["collection"] = "test-collection-2"
    test_collection = ctx.collection
    test_collection["id"] = "test-collection-2"

    await create_collection(txn_client, test_collection)
    await create_item(txn_client, test_item)

    resp = await app_client.get(
        f"/collections/{test_collection['id']}/items/{test_item['id']}"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["id"] == test_item["id"]
    assert resp_json["collection"] == test_item["collection"]

    resp = await app_client.get(f"/collections/{test_collection['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["id"] == test_collection["id"]

    resp = await app_client.post("/search", json={"collections": ["test-collection-2"]})
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert "context" in resp_json
    assert resp_json["context"]["returned"] == 1
    if matched := resp_json["context"].get("matched"):
        assert matched == 1


@pytest.mark.skip(reason="fields not implemented yet")
async def test_app_fields_extension(load_test_data, app_client, txn_client):
    item = load_test_data("test_item.json")
    txn_client.create_item(item, request=MockRequest, refresh=True)

    resp = await app_client.get("/search", params={"collections": ["test-collection"]})
    assert resp.status_code == 200
    resp_json = resp.json()
    assert list(resp_json["features"][0]["properties"]) == ["datetime"]

    txn_client.delete_item(item["id"], item["collection"])


@pytest.mark.skip(reason="Query not implemented")
async def test_app_query_extension_gt(app_client, ctx):
    params = {"query": {"proj:epsg": {"gt": ctx.item["properties"]["proj:epsg"]}}}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


@pytest.mark.skip(reason="Query not implemented")
async def test_app_query_extension_gte(app_client, ctx):
    params = {"query": {"proj:epsg": {"gte": ctx.item["properties"]["proj:epsg"]}}}
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


async def test_app_query_extension_limit_lt0(app_client):
    assert (await app_client.post("/search", json={"limit": -1})).status_code == 400


async def test_app_query_extension_limit_gt10000(app_client):
    assert (await app_client.post("/search", json={"limit": 10001})).status_code == 400


async def test_app_query_extension_limit_10000(app_client):
    params = {"limit": 10000}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200


@pytest.mark.skip(reason="Sort extension not implemented")
async def test_app_sort_extension(app_client, txn_client, ctx):
    first_item = ctx.item
    item_date = datetime.strptime(
        first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    )

    second_item = dict(first_item)
    second_item["id"] = "another-item"
    another_item_date = item_date - timedelta(days=1)
    second_item["properties"]["datetime"] = another_item_date.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    await create_item(txn_client, second_item)

    params = {
        "collections": [first_item["collection"]],
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


async def test_search_invalid_date(app_client, ctx):
    params = {
        "datetime": "2020-XX-01/2020-10-30",
        "collections": [ctx.item["collection"]],
    }

    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 400


async def test_search_point_intersects(app_client, ctx):
    point = [150.04, -33.14]
    intersects = {"type": "Point", "coordinates": point}

    params = {
        "intersects": intersects,
        "collections": [ctx.item["collection"]],
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.skip(reason="Date search not implemented")
async def test_datetime_non_interval(app_client, ctx):
    dt_formats = [
        "2020-02-12T12:30:22+00:00",
        "2020-02-12T12:30:22.00Z",
        "2020-02-12T12:30:22Z",
        "2020-02-12T12:30:22.00+00:00",
    ]

    for dt in dt_formats:
        params = {
            "datetime": dt,
            "collections": [ctx.item["collection"]],
        }

        resp = await app_client.post("/search", json=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        # datetime is returned in this format "2020-02-12T12:30:22Z"
        assert resp_json["features"][0]["properties"]["datetime"][0:19] == dt[0:19]


async def test_bbox_3d(app_client, ctx):
    australia_bbox = [106.343365, -47.199523, 0.1, 168.218365, -19.437288, 0.1]
    params = {
        "bbox": australia_bbox,
        # "collections": [ctx.item["collection"]],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.skip(reason="Line String inttersects not implemented")
async def test_search_line_string_intersects(app_client, ctx):
    line = [[150.04, -33.14], [150.22, -33.89]]
    intersects = {"type": "LineString", "coordinates": line}
    params = {
        "intersects": intersects,
        "collections": [ctx.item["collection"]],
    }

    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200

    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
