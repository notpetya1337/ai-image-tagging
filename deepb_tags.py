# Heavily borrowed from https://github.com/Epsp0/auto-tag-anime

import json
import logging
import os
import sys
from configparser import ConfigParser

import PIL
import numpy as np
import pymongo
import tensorflow as tf

from dependencies.fileops import get_image_md5

# read config
config = ConfigParser()
config.read("config.ini")
subdivs = json.loads(config.get("properties", "subdivs"))
mongocollection = config.get("storage", "mongocollection")
connectstring = config.get('storage', 'connectionstring')
mongodbname = config.get('storage', 'mongodbname')
modelpath = config.get('deepb', 'model')
tagfile = config.get('deepb', 'tagfile')
threshold = config.getfloat('deepb', 'threshold')

currentdb = pymongo.MongoClient(connectstring)[mongodbname]
collection = currentdb[mongocollection]

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logging.getLogger("PIL").setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'


class deepdanbooruModel:
    def __init__(self):
        self.tags = None
        self.model = self.load_model()

    def load_model(self):
        logger.info('Loading model...')
        try:
            model = tf.keras.models.load_model(modelpath, compile=False)
        except:
            logger.error('Model not in folder. Download it from https://github.com/KichangKim/DeepDanbooru')
            sys.exit()
        with open(tagfile, 'r') as tags_stream:
            self.tags = np.array([tag for tag in (tag.strip() for tag in tags_stream) if tag])
        logger.info('Model and tag list loaded.')
        return model

    def classify_image(self, image_path):
        try:
            image = np.array(PIL.Image.open(image_path).convert('RGB').resize((512, 512))) / 255.0
        except IOError as e:
            logger.error("Error %s processing %s", e, image_path)
            return 'fail', []

        results = self.model.predict(np.array([image])).reshape(self.tags.shape[0])
        result_tags = {}
        for i in range(len(self.tags)):
            if results[i] > threshold:
                result_tags[self.tags[i]] = results[i]
        return 'success', list(result_tags.keys())


class addAnimeTags:
    def __init__(self):
        self.model = deepdanbooruModel()

    def process_dir(self, path):
        imageextensions = (".png", ".jpg", ".gif", ".jpeg", ".webp")
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for filename in files:
                    if filename.endswith(imageextensions):
                        logger.info("Processing %s", root + '/' + filename)
                        self.write_tags_to_mongo(root + '/' + filename)
        else:
            logger.info("Processing %s", path)
            self.write_tags_to_mongo(path)

    def write_tags_to_mongo(self, path):
        md5 = get_image_md5(path)
        if collection.find_one({"md5": md5}, {"md5": 1}) is not None:
            if collection.find_one({"md5": md5, "deepbtags": {"$exists": False}}):
                status, tags = self.model.classify_image(path)
                if status == 'success':
                    collection.update_one({"md5": md5}, {"$set": {"deepbtags": tags}})
                    logger.info("Wrote tags:%s", tags)
                else:
                    logger.info("Status %s for path %s", status, path)
            else:
                logger.info("Path %s already has tags", path)


if __name__ == "__main__":
    addAnimeTags = addAnimeTags()
    for div in subdivs:
        addAnimeTags.process_dir(config.get("divs", div))
