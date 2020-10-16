import logging
from concurrent import futures
import grpc
from google.protobuf import json_format, text_format
from cisco_mdt import proto, MDTgRPCServer
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import threading
import queue

aliases = {}

class Elastic:
    def __init__(self, queue, *args, **kwargs):
        self.queue = queue
        self._continue = True
        self.es = Elasticsearch(*args, **kwargs)
    
    def bulk_generator(self):
        while self._continue:
            msg = {"_index": "mdt"}
            try:
                msg["_source"] = self.queue.get(timeout=15)
                yield msg
            except:
                break
            
    def run(self):
        while self._continue:
            try:
                helpers.bulk(self.es, self.bulk_generator())
            except Exception as ex:
                print(ex)
    
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
            "timestamp": datetime.fromtimestamp(timestamp/1000).isoformat(),
            "tags": {}
        }
        for subfield in mdt_keys.fields:
            parse_key_field(msg["tags"], subfield, "")
        
        for subfield in mdt_content.fields:
            parse_content_field(subfield, telemetry_pb.encoding_path, msg, timestamp)
        
        es_queue.put(msg)
            

if __name__ == "__main__":
    es_queue = queue.Queue()
    es = Elastic(es_queue, hosts=["10.10.20.200"])
    es_thread = threading.Thread(target=es.run, daemon=True)
    es_thread.start()

    mdt_grpc_server = MDTgRPCServer()
    mdt_grpc_server.add_mdt_callback(handle_telemetry)
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    proto.mdt_dialout_pb2_grpc.add_gRPCMdtDialoutServicer_to_server(
        mdt_grpc_server, grpc_server
    )
    grpc_server.add_insecure_port("0.0.0.0:4242")
    grpc_server.start()
    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        logging.warning("Stopping on interrupt.")
        grpc_server.stop(None)
    except Exception:
        logging.exception("Stopping due to exception!")

