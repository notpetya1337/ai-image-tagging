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
imagecount, videocount, queuedimagecount, queuedvideocount = 0, 0, 0, 0
foldercount = 0
imagecount_lock = threading.Lock()
videocount_lock = threading.Lock()
queuedimagecount_lock = threading.Lock()
queuedvideocount_lock = threading.Lock()

REDIS_CLIENT = Redis(host='localhost', port=6379, db=0)

logger.info("Loading md5s from MongoDB")
allmd5s = set([x["md5"] for x in collection.find({}, {"md5": 1, "_id": 0})])
# possibly because of entryies with no content md5?
videomd5s = set()
for x in videocollection.find({"content_md5": {"$exists": True}}, {"content_md5": 1, "_id": 0}):
    if isinstance(x["content_md5"], list):
        for y in x["content_md5"]: videomd5s.add(y)
    else: videomd5s.add(x["content_md5"])
deepbmd5s = set([x["md5"] for x in collection.find({"deepbtags": {"$exists": True}}, {"md5": 1, "_id": 0})])
visionmd5s = set([x["md5"] for x in collection.find({"vision_tags": {"$exists": True}}, {"md5": 1, "_id": 0})])
explicitmd5s = set([x["md5"] for x in collection.find({"explicit_detection": {"$exists": True}}, {"md5": 1, "_id": 0})])
logger.info("Loaded md5s from MongoDB")


def push(key, value):
    REDIS_CLIENT.rpush(key, value)


def pull(key):
    return REDIS_CLIENT.blpop(key)


def process_image_folder(workingdir, is_screenshot, subdiv):
    global imagecount, imagecount_lock, queuedimagecount, queuedimagecount_lock
    workingimages = listimages(workingdir, config.process_images)
    process_models = []
    # "Process only new" loop here
    if config.process_only_new:
        for imagepath in workingimages:
            with imagecount_lock: imagecount += 1
            process_models = []
            im_md5 = get_image_md5(imagepath)
            if im_md5 not in allmd5s:
                if subdiv in config.deepbdivs: process_models.append("deepb"), deepbmd5s.add(im_md5)
                if "vision" in config.configmodels: process_models.append("vision"), visionmd5s.add(im_md5)
                if process_models:
                    push("queue", json.dumps({"type": 'image', "path": imagepath, "is_screenshot": is_screenshot,
                                              "subdiv": subdiv, "models": process_models}))
                with queuedimagecount_lock: queuedimagecount += 1
            print(f"Processed {imagecount} images with {queuedimagecount} new ", end="\r")
        # "Process all" loop here
    elif not config.process_only_new:
        for imagepath in workingimages:
            with imagecount_lock: imagecount += 1
            process_models = []
            im_md5 = get_image_md5(imagepath)
            if "vision" in config.configmodels and im_md5 not in visionmd5s: process_models.append("vision"), visionmd5s.add(im_md5)
            if "vision" in config.configmodels and im_md5 not in explicitmd5s: process_models.append("explicit"), explicitmd5s.add(im_md5)
            if subdiv in config.deepbdivs and im_md5 not in deepbmd5s: process_models.append("deepb"), deepbmd5s.add(im_md5)
            if process_models is not None:
                push("queue", json.dumps({"type": 'image', "path": imagepath, "is_screenshot": is_screenshot,
                                          "subdiv": subdiv, "models": process_models}))
                with queuedimagecount_lock: queuedimagecount += 1
            print(f"Processed {imagecount} images with {queuedimagecount} new ", end="\r")


def process_video_folder(workingdir, subdiv):
    rootdir = config.getdiv(subdiv)
    global videocount, videocount_lock, queuedvideocount, queuedvideocount_lock
    workingvideos = listvideos(workingdir, config.process_videos)
    # Process only new video loop here
    if config.process_only_new:
        for videopath in workingvideos:
            process_models = []
            with videocount_lock: videocount += 1
            vid_md5 = get_video_content_md5(videopath)
            if vid_md5 not in videomd5s:
                if "vision" in config.configmodels: process_models.append("vision"), videomd5s.add(vid_md5)
            if process_models:
                with queuedvideocount_lock: queuedvideocount += 1
                push("queue", json.dumps({"type": 'video', "path": videopath, "subdiv": subdiv, "models": process_models}))
            print(f'Processed {videocount} videos with {queuedvideocount} new', end="\r")
    # Process all videos loop here
    elif not config.process_only_new:
        for videopath in workingvideos:
            process_models = []
            with videocount_lock: videocount += 1
            vid_md5 = get_video_content_md5(videopath)
            if "vision" in config.configmodels and vid_md5 not in visionmd5s: process_models.append("vision"), visionmd5s.add(vid_md5)
            if "vision" in config.configmodels and vid_md5 not in explicitmd5s: process_models.append("explicit"), explicitmd5s.add(vid_md5)
            if subdiv in config.deepbdivs and vid_md5 not in deepbmd5s: process_models.append("deepb"), deepbmd5s.add(vid_md5)
            if process_models:
                logger.info("Processing video %s", videopath)
                push("queue", json.dumps({"type": 'video', "path": videopath, "subdiv": subdiv, "models": process_models}))
            print(f'Processed {videocount} videos with {queuedvideocount} new', end="\r")


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
    logger.warning("All entries processed. Root divs: %s, Folder count: %s, Image count: %s, Video count: %s",
                   config.subdivs, foldercount, imagecount, videocount)
    print(queuedimagecount, "images and ", queuedvideocount, "videos queued.")
    print("Processing took ", final_time)


if __name__ == "__main__":
    main()
