SyncTo-Elasticsearch
===

A tool for **MySQL**,**MongoDB** synchronize to **ElasticSearch**.

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

| *Name*        | *Type* | *Description*        | *Required* |
|---------------|--------|----------------------|------------|
| mysql         | Object | MySQL config         | Y*         |
| mongodb       | Object | MongoDB's config     | Y*         |
| elasticsearch | Object | ElasticSearch config | Y          |

> mysql or mongodb require one

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
            ],
            "auth": {
                "username": "root",
                "password": "just4mongo",
                "database": "admin"
            }
        },
        "include_fields": [
            "nickname",
            "create_date"
        ]
    },
    "elasticsearch": {
        "index": "test-mongo-users",
        "cluster": {
            "name": "docker-cluster",
            "servers": [
                {
                    "port": 9200,
                    "host": "127.0.0.1"
                }
            ]
        },
        "creator": {
            "mapping": "{\"properties\":{\"nickname\":{\"type\":\"keyword\",\"normalizer\":\"to_lowercase\",\"null_value\":\"\"}}}",
            "settings": "{\"analysis\":{\"normalizer\":{\"to_lowercase\":{\"type\":\"custom\",\"filter\":[\"lowercase\"]}}}}"
        }
    }
}
```

``` json
{
    "mysql": {
        "server": {
            "host": "127.0.0.1",
            "port": 3306,
            "user": "root",
            "password": "just4myql",
            "database": "testdb"
        },
        "table": "users",
        "table_pkey": "id",
        "include_fields": [
            "nickname",
            "create_date"
        ]
    },
    "elasticsearch": {
        "index": "test-mysql-users",
        "cluster": {
            "name": "docker-cluster",
            "servers": [
                {
                    "port": 9200,
                    "host": "127.0.0.1"
                }
            ]
        },
        "creator": {
            "mapping": "{\"properties\":{\"nickname\":{\"type\":\"keyword\",\"normalizer\":\"to_lowercase\",\"null_value\":\"\"}}}",
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
    "source": "mongodb",
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
            "source": "mongodb",
            "status": "RUNNING"
        },
        {
            "key": "second",
            "source": "mysql",
            "status": "RUNNING"
        }
    ],
    "config": {
        "fetch_size": 1000,
        "batch_size_mb": 10,
        "batch_queue_size": 1000,
        "interval_oplog_ms": 500,
        "interval_retry_ms": 1000
    }
}
```

# Know Issues

- Reindex not safely when Worker restart in **IMPORT** status.
