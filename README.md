# cisco-mdt-collector

## Install
Installation is done with poetry:
```
pip install poetry
poetry install
```
## Usage
```
cisco-mdt-collector [-h] [--bind BIND] [--elastic-host ELASTIC_HOST] [--elastic-index ELASTIC_INDEX] [--max-workers MAX_WORKERS]

Listen for GRPC connection and send messages to an Elasticsearch backend

optional arguments:
  -h, --help            show this help message and exit
  --bind BIND, -b BIND  GRPC server binding
  --elastic-host ELASTIC_HOST, -e ELASTIC_HOST
                        Elasticsearch server
  --elastic-index ELASTIC_INDEX, -i ELASTIC_INDEX
                        Elasticsearch index
  --max-workers MAX_WORKERS, -w MAX_WORKERS
                        GRPC max workers
```


## Docker Usage
The docker compose file provided runs cisco-mdt-collect, an elasticsearch
instance and a kibana instance.
It exposes kibana on port 5601 and elasticsearch on port 9200.
```
docker-compose up
```
