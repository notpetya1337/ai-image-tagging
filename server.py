import json
import logging
import sys
import threading
from configparser import ConfigParser

import pymongo
from redis import Redis

import dependencies.vision as vision
from dependencies.fileops import get_image_content, get_image_md5

# read config
config = ConfigParser()
config.read("config.ini")
connectstring = config.get('storage', 'connectionstring')
mongodbname = config.get('storage', 'mongodbname')
mongocollection = config.get("storage", "mongocollection")
mongoscreenshotcollection = config.get("storage", "mongoscreenshotcollection")
mongovideocollection = config.get("storage", "mongovideocollection")
configmodels = json.loads(config.get("properties", "models"))
google_credentials = config.get("image-recognition", "google-credentials")
google_project = config.get("image-recognition", "google-project")

# initialize DBs
currentdb = pymongo.MongoClient(connectstring)[mongodbname]
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
videocollection = currentdb[mongovideocollection]

# Initialize models
if "deepb" in configmodels:
    import dependencies.deepb as deepb
    modelpath = config.get('deepb', 'model')
    tagfile = config.get('deepb', 'tagfile')
    threshold = config.getfloat('deepb', 'threshold')
    deepb_tagger = deepb.deepdanbooruModel(threshold, modelpath, tagfile)

# Initialize variables
tagging = vision.Tagging(google_credentials, google_project, tags_backend="google-vision")
imagecount = 0
videocount = 0
foldercount = 0
# TODO: add a rolling count on the same line with uptime, images, videos, folders
imagecount_lock = threading.Lock()
videocount_lock = threading.Lock()
# Names of likelihood from google.cloud.vision.enums
likelihood_name = (
    "UNKNOWN",
    "Very unlikely",
    "Unlikely",
    "Possible",
    "Likely",
    "Very likely",
)


# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logging.getLogger("PIL").setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REDIS_CLIENT = Redis(host='localhost', port=6379, db=0)


def pull(key):
    return REDIS_CLIENT.blpop(key)


def process_image(imagepath, workingcollection, subdiv, is_screenshot, models):
    im_md5 = get_image_md5(imagepath)
    image_content = get_image_content(imagepath)
    imagepath_array = [imagepath]
    # Check if entry exists in MongoDB, then create new entry or update entry by MD5
    entry = collection.find_one({"md5": im_md5}, {"md5": 1, "vision_tags": 1, "vision_text": 1, "deepbtags": 1, "explicit_detection": 1})
    if entry is None:
        mongo_entry = create_imagedoc(image_content, im_md5, imagepath_array, is_screenshot, subdiv, models)
        workingcollection.insert_one(mongo_entry)
        logger.info("Added new entry in MongoDB for image %s: %s\n", imagepath, mongo_entry)

    else:
        # Make sure tagging doesn't run twice
        if 'deepb' in models and 'deepbtags' not in entry:
            deepbtags = deepb_tagger.classify_image(imagepath)
            collection.update_one({"md5": im_md5}, {"$set": {"deepbtags": deepbtags[1]}})
        if 'vision' in models and entry['vision_tags'] is None:
            logger.warning("Not processing vision tags yet")
            # collection.update_one()
        if 'deepdetect' in models and entry['deepdetect_tags'] is None:
            logger.warning("Not processing deepdetect tags yet")
            # collection.update_one()

    logger.info("Updated MongoDB entry for image %s \n", imagepath)


def create_imagedoc(image_content, im_md5, image_array, is_screenshot, subdiv, models):
    tags, text, safe, deepbtags, explicit_detection = None, None, None, None, None
    if 'deepb' in models and 'deepb' not in configmodels:
        logger.error("Client requested DeepB tags but DeepB is disabled in config")
    if 'deepb' in models and is_screenshot != 1:
        deepbtags = deepb_tagger.classify_image(image_array[0])
        deepbtags = deepbtags[1]
    if 'vision' in models:
        text = tagging.get_text(image_binary=image_content)
        text = [text[0]]
        if is_screenshot != 1:
            tags = tagging.get_tags(image_binary=image_content)
            safe = tagging.get_explicit(image_binary=image_content)
            explicit_detection = {
                "adult": f"{likelihood_name[safe.adult]}",
                "medical": f"{likelihood_name[safe.medical]}",
                "spoofed": f"{likelihood_name[safe.spoof]}",
                "violence": f"{likelihood_name[safe.violence]}",
                "racy": f"{likelihood_name[safe.racy]}",
            }
            explicit_detection = [explicit_detection][0]
    if 'deepdetect' in models:
        logger.warning("Not processing deepdetect tags yet")

    mongo_entry = {
        "md5": im_md5,
        "vision_tags": tags,
        "vision_text": text,
        "explicit_detection": explicit_detection,
        "deepbtags": deepbtags,
        "path": image_array,
        "subdiv": subdiv,
        "is_screenshot": is_screenshot,
    }

    logger.info("Generated MongoDB entry: %s", mongo_entry)
    return mongo_entry


while True:
    logger.info("Waiting for job")
    job = json.loads(pull("queue")[1])
    if job['type'] == 'image':
        print("Processing image, job is", job)
        if job['subdiv'] == 'screenshot':
            process_image(job['path'], screenshotcollection, job['subdiv'], job['is_screenshot'], job['models'])
        else:
            process_image(job['path'], collection, job['subdiv'], job['is_screenshot'], job['models'])
