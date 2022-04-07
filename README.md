# stac-fastapi-caching
Stac-fastapi built on Tile38 to support caching. This code is built on top of stac-fastapi-elasticsearch 0.1.0 with pyle38, a Python client for tile38. Tile38 itself is built on top of Redis to support geospatial queries. 

### References

- tile38 - https://tile38.com/
- pyle38 - https://github.com/iwpnd/pyle38
- stac-fastapi-elasticsearch - https://github.com/stac-utils/stac-fastapi-elasticsearch

### Important

This code is not production-ready. CRUD routes work with items and collections. Bbox queries should work, and point and polygon intersection. Searching lists of item and collection ids is also functional.