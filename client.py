import concurrent.futures
import datetime
import json
import logging
import os
import sys
import threading
import time
from configparser import ConfigParser

import pymongo
from redis import Redis

from dependencies.fileops import get_image_md5, get_video_content_md5, listdirs, listimages, listvideos

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logging.getLogger("PIL").setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# read config
config = ConfigParser()
config.read("config.ini")
subdivs = json.loads(config.get("properties", "subdivs"))
process_videos = config.getboolean("storage", "process_videos")
process_images = config.getboolean("storage", "process_images")
configmodels = json.loads(config.get("properties", "models"))
threads = config.getint("properties", "threads")
connectstring = config.get('storage', 'connectionstring')
mongodbname = config.get('storage', 'mongodbname')
mongocollection = config.get("storage", "mongocollection")
mongoscreenshotcollection = config.get("storage", "mongoscreenshotcollection")
mongovideocollection = config.get("storage", "mongovideocollection")
process_only_new = config.getboolean("flags", "process_only_new")
deepbdivs = json.loads(config.get('deepb', 'deepbdivs'))


# initialize DBs
currentdb = pymongo.MongoClient(connectstring)[mongodbname]
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
videocollection = currentdb[mongovideocollection]
collection.create_index([("md5", pymongo.TEXT)], name="md5_index", unique=True)
collection.create_index("vision_tags")
screenshotcollection.create_index([("md5", pymongo.TEXT)], name="md5_index", unique=True)
videocollection.create_index("content_md5")
videocollection.create_index("vision_tags")

# Initialize variables
imagecount = 0
videocount = 0
foldercount = 0
imagecount_lock = threading.Lock()
videocount_lock = threading.Lock()

REDIS_CLIENT = Redis(host='localhost', port=6379, db=0)

logger.info("Loading md5s from MongoDB")
imagemd5s = set([x["md5"] for x in collection.find({}, {"md5": 1, "_id": 0})])
# TODO: repeat above but with filtering for Vision, DeepB, and DeepDetect tags, check config for flags
logger.info("Loaded md5s from MongoDB")


def push(key, value):
    REDIS_CLIENT.rpush(key, value)


def pull(key):
    return REDIS_CLIENT.blpop(key)


def process_image_folder(workingdir, is_screenshot, subdiv, process_new_only):
    global imagecount, imagecount_lock
    workingimages = listimages(workingdir, process_images)
    process_models = []
    if subdiv in deepbdivs:
        process_models.append("deepb")
    if "vision" in configmodels:
        process_models.append("vision")
    for imagepath in workingimages:
        with imagecount_lock:
            imagecount += 1
        im_md5 = get_image_md5(imagepath)
        if process_new_only:
            if im_md5 not in imagemd5s:
                push("queue", json.dumps({"type": 'image', "path": imagepath, "is_screenshot": is_screenshot,
                                          "subdiv": subdiv, "models": process_models}))
        else:
            push("queue", json.dumps({"type": 'image', "path": imagepath, "is_screenshot": is_screenshot,
                                      "subdiv": subdiv, "models": process_models}))


def process_video_folder(workingdir, subdiv):
    rootdir = config.get("divs", subdiv)
    global videocount, videocount_lock
    workingvideos = listvideos(workingdir, process_videos)
    for videopath in workingvideos:
        with videocount_lock:
            videocount += 1
        process_video(videopath, subdiv, rootdir)


def process_video(videopath, subdiv, rootdir=""):
    video_content_md5 = str(get_video_content_md5(videopath))
    relpath = os.path.relpath(videopath, rootdir)


def main():
    global foldercount
    start_time = time.time()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
    for div in subdivs:
        rootdir = config.get("divs", div)
        allfolders = listdirs(rootdir)
        for _ in allfolders:  # spawn thread per entry here
            workingdir = allfolders.pop(0)
            foldercount += 1
            # TODO: limit number of threads
            if process_images:
                if workingdir.lower().find("screenshot") != -1:
                    is_screenshot = 1
                else:
                    is_screenshot = 0
                pool.submit(process_image_folder(workingdir, is_screenshot, div, process_only_new))
            # if process_videos:
            #     pool.submit(process_video_folder(workingdir, div))

    # Wait until all threads exit
    pool.shutdown(wait=True)
    elapsed_time = time.time() - start_time
    final_time = str(datetime.timedelta(seconds=elapsed_time))
    logger.warning("All entries processed. Root divs: %s, Folder count: %s", subdivs, foldercount)
    print(imagecount, "images and ", videocount, "videos queued.")
    print("Processing took ", final_time)


if __name__ == "__main__":
    main()
