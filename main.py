import logging
from concurrent import futures
import grpc
from google.protobuf import json_format, text_format
from cisco_mdt import proto, MDTgRPCServer
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import threading
import queue
import argparse

logger = logging.getLogger(__name__)
aliases = {}

class Elastic:
    def __init__(self, queue, index, *args, **kwargs):
        self.queue = queue
        self.index = index
        self._continue = True
        self.es = Elasticsearch(*args, **kwargs)
    
    def bulk_generator(self):
        while self._continue:
            msg = {"_index": self.index}
            try:
                msg["_source"] = self.queue.get(timeout=15)
                yield msg
            except:
                break
            
    def run(self):
        while self._continue:
            logger.debug("Starting elasticsearch bulk insert")
            try:
                helpers.bulk(self.es, self.bulk_generator())
            except Exception as ex:
                logger.critical(str(ex))
    
    def stop(self):
        self._continue = False


def decode_value(field):
    value_type = field.WhichOneof('value_by_type')
    v = None if value_type is None else getattr(field, value_type)
    return v

def parse_key_field(tags, field, prefix):
    localname = field.name.replace("-", "_")
    
    if len(localname) == 0:
        name = prefix
    elif len(prefix) == 0:
        name = localname
    else:
        name = prefix + "/" + localname
    
    if len(name) == 0:
        return 
    tag = decode_value(field)
    if tag is not None:
        tags[name] = tag
    
    for subfield in field.fields:
        parse_key_field(tags, field, name)

def parse_content_field(field, path, msg, timestamp):
    name = field.name.replace("-", "_")
    if len(name) == 0:
        name = path
    else:
        name = path + "/" + name
    
    value = decode_value(field)
    if value is not None:
        alias = aliases.get(name, name).replace("/","_").replace(":", "_")
        msg[alias] = value
    
    for subfield in field.fields:
        parse_content_field(subfield, name, msg, timestamp)


def handle_telemetry(request):
    print(f"Got a request", es_queue.qsize())
    try:
        telemetry_pb = proto.telemetry_bis_pb2.Telemetry()
        telemetry_pb.ParseFromString(request.data)
    except Exception as e:
        logging.error(f"MDT failed to decode: {e}")
        return
    
    for gpbkv in telemetry_pb.data_gpbkv:
        timestamp = gpbkv.timestamp or telemetry_pb.timestamp
        
        try:
            mdt_keys = [f for f in gpbkv.fields if f.name == "keys"][0]
            mdt_content = [f for f in gpbkv.fields if f.name == "content"][0]
        except:
            logging.error(f"Message missing keys or content: {gpbkv}")
        
        msg = {
            "telemetry_source": telemetry_pb.node_id_str,
            "telemetry_subscription": telemetry_pb.subscription_id_str ,
            "telemetry_path": telemetry_pb.encoding_path,
            "@timestamp": datetime.fromtimestamp(timestamp/1000).isoformat(),
            "tags": {}
        }
        for subfield in mdt_keys.fields:
            parse_key_field(msg["tags"], subfield, "")
        
        for subfield in mdt_content.fields:
            parse_content_field(subfield, telemetry_pb.encoding_path, msg, timestamp)
        
        es_queue.put(msg)

def parse_args():
    parser = argparse.ArgumentParser(description='Listen for GRPC connection and send messages to an Elasticsearch backend')
    parser.add_argument("--bind", "-b", default="0.0.0.0:8080", help="GRPC server binding")
    parser.add_argument("--elastic-host", "-e", action="append", help="Elasticsearch server")
    parser.add_argument("--elastic-index", "-i", default="mdt", help="Elasticsearch index")
    parser.add_argument("--max-workers", "-w", default=10, type=int, help="GRPC max workers")

    args = parser.parse_args()

    return args

if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    args = parse_args()
    es_queue = queue.Queue()
    es = Elastic(es_queue, args.elastic_index, hosts=args.elastic_host)
    es_thread = threading.Thread(target=es.run, daemon=True)
    es_thread.start()

    mdt_grpc_server = MDTgRPCServer()
    mdt_grpc_server.add_mdt_callback(handle_telemetry)
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.max_workers))
    proto.mdt_dialout_pb2_grpc.add_gRPCMdtDialoutServicer_to_server(
        mdt_grpc_server, grpc_server
    )
    grpc_server.add_insecure_port(args.bind)
    grpc_server.start()
    try:
        logger.info("Starting grpc server")
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        logger.warning("Stopping on interrupt.")
        grpc_server.stop(None)
    except Exception as ex:
        logger.exception("Stopping due to exception!" + str(ex))

