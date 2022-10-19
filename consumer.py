#!/usr/bin/python
import datetime
import logging
import os
import sys
import time
from redisqueue import RedisQueue

from configparser import SafeConfigParser
from searchengine import SEngine
from tagging import Tagging


def get_image_content(image_path):
    image = open(image_path, 'rb')
    return image.read()


# logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.debug("logging started")
logger = logging.getLogger(__name__)

# Config Parsing
config = SafeConfigParser()
config.read('config.ini')

# Queue 
queue = RedisQueue(config)
tagging = Tagging(config)
es_engine = SEngine(config)
# consuming forever
while True:
    # ensure all arrays are empty
    doc = {}
    tags = []

    # get image to process
    image = queue.pop_from_queue()
    if not image:
        time.sleep(0.25)
        continue
    logger.info('received image=%s', image)
    image_content = get_image_content(image)
    logger.info('getting tags for image=%s', image)
    tags = tagging.get_tags(image_binary=image_content)

    # Prepare ElasticSearch Document
    doc['image_type'] = os.path.splitext(image)[1]
    doc['image_fname'] = os.path.basename(image)
    doc['image_path'] = image
    doc['uploaded_at'] = datetime.datetime.fromtimestamp(
        int(os.path.getmtime(image))
    ).strftime('%Y-%m-%d %H:%M:%S')
    doc['timestamp'] = int(time.time())
    doc['en_labels'] = tags
    logger.info('pushing all details to elasticsearch for image=%s', image)
    if not es_engine.push_to_es(doc=doc):
        queue.add_to_queue(queue_name='failed_queue', image=image)
    # TODO: fix elasticsearch unexpected argument error
