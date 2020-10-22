import elasticsearch
import elasticsearch.helpers
import datetime
import argparse

from .logger import logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Listen for GRPC connection and send messages to an Elasticsearch backend"
    )
    parser.add_argument(
        "--bind", "-b", default="0.0.0.0:8080", help="GRPC server binding"
    )
    parser.add_argument(
        "--elastic-host", "-e", action="append", help="Elasticsearch server"
    )
    parser.add_argument(
        "--elastic-index", "-i", default="mdt", help="Elasticsearch index"
    )
    parser.add_argument(
        "--max-workers", "-w", default=10, type=int, help="GRPC max workers"
    )

    args = parser.parse_args()

    return args


class Elastic:
    def __init__(self, queue, index="mdt", max_wait=10, *args, **kwargs):
        self.queue = queue
        self.index = index
        self.max_wait = max_wait
        self._continue = True
        self.es = elasticsearch.Elasticsearch(*args, **kwargs)

    def bulk_generator(self):
        start_time = datetime.datetime.now()
        while self._continue:
            msg = {"_index": self.index}
            try:
                msg["_source"] = self.queue.get(timeout=15)
                yield msg
                elapsed = datetime.datetime.now() - start_time
                if elapsed.total_seconds() > self.max_wait:
                    logger.debug("Bulk generator reached max wait")
                    return
            except Exception:
                break

    def run(self):
        while self._continue:
            logger.debug("Starting elasticsearch bulk insert")
            try:
                elasticsearch.helpers.bulk(self.es, self.bulk_generator())
            except Exception as ex:
                logger.critical(str(ex))

    def stop(self):
        self._continue = False
