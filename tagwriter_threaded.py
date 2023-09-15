import concurrent.futures
import datetime
import json
import logging
import sys
import threading
import time
from configparser import ConfigParser

import exiftool
import pymongo
from bson.json_util import dumps, loads

from dependencies.fileops import (get_image_md5, get_video_content_md5, listdirs, listimages, listvideos)

# read config
config = ConfigParser()
config.read("config.ini")
subdivs = json.loads(config.get("properties", "subdivs"))
mongocollection = config.get("storage", "mongocollection")
mongoscreenshotcollection = config.get("storage", "mongoscreenshotcollection")
mongovideocollection = config.get("storage", "mongovideocollection")
process_videos = config.getboolean("storage", "process_videos")
process_images = config.getboolean("storage", "process_images")
logging_level = config.get("logging", "loglevel")
maxlength = config.getint("properties", "maxlength")
threads = config.getint("properties", "threads")
connectstring = config.get('storage', 'connectionstring')
mongodbname = config.get('storage', 'mongodbname')

# initialize logger
log_level_dict = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logging.getLogger("PIL").setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

# initialize DBs
currentdb = pymongo.MongoClient(connectstring)[mongodbname]
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
videocollection = currentdb[mongovideocollection]

# Initialize variables
imagecount = 0
videocount = 0
foldercount = 0
folderlist = []
imagecount_lock = threading.Lock()
videocount_lock = threading.Lock()


def getimagetags(md5, workingcollection, is_screenshot):
    text = loads(dumps(workingcollection.find_one({"md5": md5}, {"vision_text": 1, "_id": 0})))
    tagsjson = loads(dumps(workingcollection.find_one({"md5": md5}, {"vision_tags": 1, "_id": 0})))
    deepbtags = loads(dumps(workingcollection.find_one({"md5": md5}, {"deepbtags": 1, "_id": 0})))
    if is_screenshot == 1:
        explicit_results = []
    else:
        explicit_mongo = workingcollection.find_one({"md5": md5}, {"explicit_detection": 1, "_id": 0})
        if explicit_mongo:
            detobj = explicit_mongo["explicit_detection"]
            detobj = detobj[0]
            explicit_results = (
                f"Adult: {detobj['adult']}",
                f"Medical: {detobj['medical']}",
                f"Spoofed: {detobj['spoofed']}",
                f"Violence: {detobj['violence']}",
                f"Racy: {detobj['racy']}",
            )
        else:
            explicit_results = []

    if tagsjson:
        tagsjson["vision_tags"].extend(explicit_results)
        tagsjson["vision_tags"].extend(deepbtags["deepbtags"])
        tags_list = tagsjson.get("vision_tags")
        logger.info("Tags are %s", tags_list)
    else:
        tags_list = []
    if text:
        text_list = text.get("vision_text").replace("\n", "\\n")
        logger.info("Text is %s", text_list)
    else:
        text_list = []
    return tags_list, text_list


def getvideotags(content_md5):
    text_array = loads(dumps(videocollection.find_one({"content_md5": content_md5}, {"vision_text": 1, "_id": 0})))
    try:
        explicit_mongo = videocollection.find_one({"content_md5": content_md5}, {"explicit_detection": 1, "_id": 0})
        detobj = explicit_mongo["explicit_detection"]
    except (KeyError, TypeError):
        logger.warning("Explicit tags not found for %s", content_md5)
        detobj = []
    tagsjson = loads(dumps(videocollection.find_one({"content_md5": content_md5}, {"vision_tags": 1, "_id": 0})))
    if tagsjson:
        tagsjson["vision_tags"].extend(detobj)
        tags_list = tagsjson.get("vision_tags")
        logger.info("Tags are %s", tags_list)
    else:
        tags_list = []
    if text_array:
        text_list = (" ".join(text_array.get("vision_text"))).replace("\n", "\\n")
        # Truncate text at set number of characters
        text_list = ((text_list[:maxlength] + " truncated...") if len(text_list) > maxlength else text_list)
        logger.info("Text is %s", text_list)
    else:
        text_list = []

    return tags_list, text_list


def writeimagetags(path, tags, text, et):
    # Tags go to Subject, comma separated
    # Text goes to Description for now
    if tags and text:
        try:
            # TODO: check back and see if -ec is necessary
            et.set_tags(path, tags={"Subject": tags, "xmp:Title": text}, params=["-P", "-overwrite_original"])
        except exiftool.exceptions.ExifToolExecuteError as e:
            logger.warning('Error "%s " writing tags, Exiftool output was %s', e, et.last_stderr)
    elif tags:
        try:
            et.set_tags(path, tags={"Subject": tags}, params=["-P", "-overwrite_original"])
        except exiftool.exceptions.ExifToolExecuteError as e:
            logger.warning('Error "%s " writing tags, Exiftool output was %s', e, et.last_stderr)


def writevideotags(path, tags, text, et):
    if tags:
        try:
            et.set_tags(path, tags={"Subject": tags}, params=["-P", "-overwrite_original"])
        except exiftool.exceptions.ExifToolExecuteError as e:
            logger.warning('Error "%s " writing tags, Exiftool output was %s', e, et.last_stderr)
    if text:
        try:
            et.set_tags(path, tags={"Title": text}, params=["-P", "-overwrite_original"])
        except exiftool.exceptions.ExifToolExecuteError as e:
            logger.warning('Error "%s " writing tags, Exiftool output was %s', e, et.last_stderr)


def processimagefolder(workingdir, workingcollection, is_screenshot, et):
    global imagecount, imagecount_lock
    workingimages = listimages(workingdir, process_images)
    for imagepath in workingimages:
        logger.info("Processing image %s", imagepath)
        with imagecount_lock:
            imagecount += 1
        im_md5 = get_image_md5(imagepath)
        tags, text = getimagetags(im_md5, workingcollection, is_screenshot)
        writeimagetags(imagepath, tags, text, et)


def processvideofolder(workingdir, et):
    global videocount, videocount_lock
    workingvideos = listvideos(workingdir, process_videos)
    for videopath in workingvideos:
        with videocount_lock:
            videocount += 1
        video_md5 = get_video_content_md5(videopath)
        tags, text = getvideotags(video_md5)
        writevideotags(videopath, tags, text, et)


def main():
    global foldercount, folderlist
    start_time = time.time()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
    et = exiftool.ExifToolHelper(logger=logging.getLogger(__name__).setLevel(logging.INFO), encoding="utf-8")
    for div in subdivs:
        rootdir = config.get("divs", div)
        allfolders = listdirs(rootdir)
        logger.info("allfolders = %s", allfolders)
        while allfolders:  # spawn thread per entry here
            workingdir = allfolders.pop()
            folderlist.append(workingdir)
            foldercount += 1
            if process_images:
                if workingdir.lower().find("screenshot") != -1:
                    is_screenshot = 1
                    workingcollection = screenshotcollection
                else:
                    is_screenshot = 0
                    workingcollection = collection
                et = exiftool.ExifToolHelper(
                    logger=logging.getLogger(__name__).setLevel(logging.INFO),
                    encoding="utf-8",
                )
                pool.submit(processimagefolder(workingdir, workingcollection, is_screenshot, et))
            if process_videos:
                et = exiftool.ExifToolHelper(
                    logger=logging.getLogger(__name__).setLevel(logging.INFO),
                    encoding="utf-8",
                )
                pool.submit(processvideofolder(workingdir, et))
    pool.shutdown(wait=True)
    et.terminate()
    elapsed_time = time.time() - start_time
    final_time = str(datetime.timedelta(seconds=elapsed_time))

    logger.info("All entries processed. Root divs: %s, Folder count: %s", subdivs, foldercount)
    logger.info("Folders processed: %s", folderlist)
    print(imagecount, "images and ", videocount, "videos processed.")
    print("Processing took ", final_time)


if __name__ == "__main__":
    main()
