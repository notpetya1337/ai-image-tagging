import concurrent.futures
import datetime
import json
import logging
import sys
import threading
import time

import pymongo
from redis import Redis

from dependencies.configops import MainConfig
from dependencies.fileops import get_image_md5, get_video_content_md5, listdirs, listimages, listvideos

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# read config
config = MainConfig("config.ini")

# initialize DBs
currentdb = pymongo.MongoClient(config.connectstring)[config.mongodbname]
collection = currentdb[config.mongocollection]
screenshotcollection = currentdb[config.mongoscreenshotcollection]
videocollection = currentdb[config.mongovideocollection]
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
allmd5s = set([x["md5"] for x in collection.find({}, {"md5": 1, "_id": 0})])
# TODO: repeat above with filtering for Vision, DeepB, and DeepDetect tags, check config for flags, fetch only if needed
deepbmd5s = set([x["md5"] for x in collection.find({"deepbtags": {"$exists": True}}, {"md5": 1, "_id": 0})])
visionmd5s = set([x["md5"] for x in collection.find({"vision_tags": {"$exists": True}}, {"md5": 1, "_id": 0})])
explicitmd5s = set([x["md5"] for x in collection.find({"explicit_detection": {"$exists": True}}, {"md5": 1, "_id": 0})])
videomd5s = set([x["content_md5"] for x in videocollection.find({}, {"content_md5": 1, "_id": 0})])
logger.info("Loaded md5s from MongoDB")


def push(key, value):
    REDIS_CLIENT.rpush(key, value)


def pull(key):
    return REDIS_CLIENT.blpop(key)


def process_image_folder(workingdir, is_screenshot, subdiv):
    global imagecount, imagecount_lock
    workingimages = listimages(workingdir, config.process_images)
    process_models = []
    # "Process only new" loop here
    if config.process_only_new:
        for imagepath in workingimages:
            with imagecount_lock: imagecount += 1
            im_md5 = get_image_md5(imagepath)
            if subdiv in config.deepbdivs: process_models.append("deepb")
            if "vision" in config.configmodels: process_models.append("vision")
            if im_md5 not in allmd5s:
                push("queue", json.dumps({"type": 'image', "path": imagepath, "is_screenshot": is_screenshot,
                                          "subdiv": subdiv, "models": process_models}))
        # "Process all" loop here
    elif not config.process_only_new:
        for imagepath in workingimages:
            with imagecount_lock:
                imagecount += 1
            im_md5 = get_image_md5(imagepath)
            if "vision" in config.configmodels and im_md5 not in visionmd5s: process_models.append("vision")
            if "vision" in config.configmodels and im_md5 not in explicitmd5s: process_models.append("explicit")
            if subdiv in config.deepbdivs and im_md5 not in deepbmd5s: process_models.append("deepb")
            push("queue", json.dumps({"type": 'image', "path": imagepath, "is_screenshot": is_screenshot,
                                      "subdiv": subdiv, "models": process_models}))


def process_video_folder(workingdir, subdiv):
    rootdir = config.getdiv(subdiv)
    process_models = []
    global videocount, videocount_lock
    workingvideos = listvideos(workingdir, config.process_videos)
    # Process only new video loop here
    if config.process_only_new:
        for videopath in workingvideos:
            with videocount_lock: videocount += 1
            vid_md5 = get_video_content_md5(videopath)
            if "vision" in config.configmodels and vid_md5 not in videomd5s: process_models.append("vision")
            push("queue", json.dumps({"type": 'video', "path": videopath, "subdiv": subdiv, "models": process_models}))
    # Process all video loop here
    # TODO


def main():
    global foldercount
    start_time = time.time()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=config.threads)
    for div in config.subdivs:
        rootdir = config.getdiv(div)
        allfolders = listdirs(rootdir)
        for _ in allfolders:  # spawn thread per entry here
            workingdir = allfolders.pop(0)
            foldercount += 1
            # TODO: limit number of threads
            if config.process_images:
                if workingdir.lower().find("screenshot") != -1:
                    is_screenshot = 1
                else:
                    is_screenshot = 0
                pool.submit(process_image_folder(workingdir, is_screenshot, div))
            if config.process_videos:
                pool.submit(process_video_folder(workingdir, div))

    # Wait until all threads exit
    pool.shutdown(wait=True)
    elapsed_time = time.time() - start_time
    final_time = str(datetime.timedelta(seconds=elapsed_time))
    logger.warning("All entries processed. Root divs: %s, Folder count: %s", config.subdivs, foldercount)
    print(imagecount, "images and ", videocount, "videos queued.")
    print("Processing took ", final_time)


if __name__ == "__main__":
    main()
