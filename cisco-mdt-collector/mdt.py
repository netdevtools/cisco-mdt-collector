import logging
import grpc
from concurrent import futures

from cisco_mdt import proto
from mdt_grpc_server import MDTgRPCServer
import datetime
import inspect


def get_mdt_server(reader, bind, max_workers):
    mdt_grpc_server = MDTgRPCServer()
    mdt_grpc_server.add_mdt_callback(reader.read_messages)
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    proto.mdt_dialout_pb2_grpc.add_gRPCMdtDialoutServicer_to_server(
        mdt_grpc_server, grpc_server
    )
    grpc_server.add_insecure_port(bind)

    return grpc_server


class MDTReader:
    def __init__(self, es_queue, aliases={}, logger=None):
        self.es_queue = es_queue
        self.aliases=aliases
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def _decode_value(self, field):
        value_type = field.WhichOneof('value_by_type')
        v = None if value_type is None else getattr(field, value_type)
        return v

    def _parse_key_field(self, tags, field, prefix):
        localname = field.name.replace("-", "_")
        
        if len(localname) == 0:
            name = prefix
        elif len(prefix) == 0:
            name = localname
        else:
            name = prefix + "/" + localname
        
        if len(name) == 0:
            return 
        tag = self._decode_value(field)
        if tag is not None:
            tags[name] = tag
        
        for subfield in field.fields:
            self._parse_key_field(tags, field, name)

    def _parse_content_field(self, field, path, msg, timestamp):
        name = field.name.replace("-", "_")
        if len(name) == 0:
            name = path
        else:
            name = path + "/" + name
        
        value = self._decode_value(field)
        if value is not None:
            alias = self.aliases.get(name, name).replace("/","_").replace(":", "_")
            msg[alias] = value
        
        for subfield in field.fields:
            self._parse_content_field(subfield, name, msg, timestamp)


    def read_messages(self, request):
        self.logger.debug("Got messages")
        try:
            telemetry_pb = proto.telemetry_bis_pb2.Telemetry()
            telemetry_pb.ParseFromString(request.data)
        except Exception as e:
            self.logger.error(f"MDT failed to decode: {e}")
            return
        

        for gpbkv in telemetry_pb.data_gpbkv:
            timestamp = gpbkv.timestamp or telemetry_pb.timestamp
            
            try:
                mdt_keys = [f for f in gpbkv.fields if f.name == "keys"][0]
                mdt_content = [f for f in gpbkv.fields if f.name == "content"][0]
            except:
                self.logger.error(f"Message missing keys or content: {gpbkv}")
                return
            msg = {
                "telemetry_source": telemetry_pb.node_id_str,
                "telemetry_subscription": telemetry_pb.subscription_id_str ,
                "telemetry_path": telemetry_pb.encoding_path,
                "@timestamp": datetime.datetime.fromtimestamp(timestamp/1000).isoformat(),
                "tags": {}
            }
            for subfield in mdt_keys.fields:
                self._parse_key_field(msg["tags"], subfield, "")
            
            for subfield in mdt_content.fields:
                self._parse_content_field(subfield, telemetry_pb.encoding_path, msg, timestamp)
            
            self.es_queue.put(msg)
        
