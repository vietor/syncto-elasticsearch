MongoDB-Sync-Elasticsearch
===

A tool for **MongoDB** synchronize to **ElasticSearch**.

# REST API

## Summary

> Data transfer via HTTP request.
> Character encoding is unified with UTF-8.
> Request parameters, using the application/json format.
> Request results, using the application/json format, and each API is consistent.

## Create Worker

### Request

[PUT] /_worker/{{key}}/_meta

### Response

|*Name*|*Type*|*Description*|*Required*|
|---|---|---|---|
|mongodb|Object|MongoDB's config|Y|
|elasticsearch|Object|ElasticSearch config|Y|

``` json
{
    "mongodb": {
        "db": "testdb",
        "collection": "users",
        "cluster": {
            "servers": [
                {
                    "port": 27017,
                    "host": "127.0.0.1"
                }
            ]
        },
        "include_fields": [
            "_id",
            "nickname",
            "create_date"
        ]
    },
    "elasticsearch": {
        "index": "test-users",
        "cluster": {
            "name": "cluster1",
            "servers": [
                {
                    "port": 9200,
                    "host": "127.0.0.1"
                }
            ]
        },
        "creator": {
            "mapping": "{\"properties\":{\"title\":{\"type\":\"keyword\",\"normalizer\":\"to_lowercase\",\"null_value\":\"\"}}}",,
            "settings": "{\"analysis\":{\"normalizer\":{\"to_lowercase\":{\"type\":\"custom\",\"filter\":[\"lowercase\"]}}}}"
        }
    }
}
```

## Dump Worker metadata

### Request

[GET] /_worker/{{key}}/_meta

## Dump Worker Status

### Request

[GET] /_worker/{{key}}/_status

### Response

|*Name*|*Type*|*Description*|
|---|---|---|
|key|string|Worker Key|
|status|string|Status Code|
|summary|Object|Summary of Worker|

``` json
{
    "key": "first",
    "status": "RUNNING",
    "summary": {
        "status": {
            "step": "OPLOG"
        },
        "import": {
            "completed_ts": "1472533804",
            "completed": "ok",
            "count": "570608",
            "duration": "36"
        },
        "oplog": {
            "shard1": "{\"time\":1472613463,\"seq\":1}",
            "shard2": "{\"time\":1472613463,\"seq\":1}"
        }
    }
}
```

## Stop Worker

### Request

[POST] /_worker/{{key}}/stop

## Start Worker

### Request

[POST] /_worker/{{key}}/start

## Delete Worker

### Request

[DELETE] /_worker/{{key}}

## Dump System Status

### Request

[GET] /_status

### Response

|*Name*|*Type*|*Description*|
|---|---|---|
|version|string|Version|
|uptime|long|Uptime (seconds)|
|timestamp|long|Current timestamp|
|workers|Array(worker)|Array of Workers|

#### worker
|*Name*|*Type*|*Description*|
|---|---|---|
|key|string|Worker Key|
|status|string|Status Code|

``` json
{
    "version": "2.0",
    "uptime": 1933,
    "timestamp": 1472614196,
    "workers": [
        {
            "key": "first",
            "status": "RUNNING"
        },
        {
            "key": "second",
            "status": "RUNNING"
        }
    ]
}
```

# Know Issues

- Reindex not safely when Worker restart in **IMPORT** status.
