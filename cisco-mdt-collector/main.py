import logging

import threading
import queue


from logger import logger, setup_logger
from mdt import MDTReader, get_mdt_server
from helpers import Elastic, parse_args


def test(*args, **kwargs):
    print("-------------------")


if __name__ == "__main__":
    args = parse_args()
    setup_logger(logger)

    es_queue = queue.Queue()
    es = Elastic(es_queue, args.elastic_index, hosts=args.elastic_host)
    es_thread = threading.Thread(target=es.run, daemon=True)
    es_thread.start()

    mdt_reader = MDTReader(es_queue, logger=logger)

    grpc_server = get_mdt_server(mdt_reader, args.bind, args.max_workers)
    grpc_server.start()
    try:
        logger.info("Starting grpc server")
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        logger.warning("Stopping on interrupt.")
        grpc_server.stop(None)
    except Exception as ex:
        logger.exception("Stopping due to exception!" + str(ex))

