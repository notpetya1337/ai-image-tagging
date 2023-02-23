import logging
import io
import os
import sys
import time
import datetime
from configparser import ConfigParser
from tagging import Tagging
from PIL import Image
import hashlib
from mongoclient import get_database
import pymongo
from videotagging import VideoData
from bson.json_util import dumps, loads

import exiftagger

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

config = ConfigParser()
config.read('config.ini')

# Tags go to Subject, comma separated
# Text goes to Description for now


# read config
config = ConfigParser()
config.read('config.ini')
subdiv = config.get('properties', 'subdiv')
rootdir = config.get('divs', subdiv)
sqldb = config.get('storage', 'sqlitedb')
mongocollection = config.get('storage', 'mongocollection')
mongoscreenshotcollection = config.get('storage', 'mongoscreenshotcollection')
mongodownloadscollection = config.get('storage', 'mongodownloadscollection')
mongoartcollection = config.get('storage', 'mongoartcollection')
mongovideocollection = config.get('storage', 'mongovideocollection')
process_videos = config.getboolean('storage', 'process_videos')
process_images = config.getboolean('storage', 'process_images')
check_mongo = config.getboolean('storage', 'check_mongo')

# initialize DBs
currentdb = get_database()
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
downloadcollection = currentdb[mongodownloadscollection]
artcollection = currentdb[mongoartcollection]
videocollection = currentdb[mongovideocollection]


# list all subdirectories in a given folder
def listdirs(folder):
    internallist = [folder]
    for root, directories, files in os.walk(folder):
        for directory in directories:
            internallist.append(os.path.join(root, directory))
    return internallist


# list all images in a given folder
def listimages(subfolder):
    imageextensions = (".png", ".jpg", ".gif", ".jpeg")
    internallist = []
    if not process_images:
        logger.info("Not processing images")
        return internallist
    for file in os.listdir(subfolder):
        if file.endswith(imageextensions):
            imagepath = os.path.join(subfolder, file)
            internallist.append(imagepath)
    return internallist


def listvideos(subfolder):
    videoextensions = (".mp4", ".mov", ".mkv") # ".webm",
    internallist = []
    if not process_videos:
        logger.info("Not processing videos")
        return internallist
    for file in os.listdir(subfolder):
        if file.endswith(videoextensions):
            videopath = os.path.join(subfolder, file)
            internallist.append(videopath)
    return internallist


# open an image at a given path
def get_image_content(image_path):
    image = open(image_path, 'rb')
    return image.read()


def get_video_content(video_path):
    video = io.open(video_path, 'rb')
    return video.read()


def get_md5(image_path):
    try:
        im = Image.open(image_path)
        return hashlib.md5(im.tobytes()).hexdigest()
    except OSError:
        return "corrupt"
    except SyntaxError:
        return "corrupt"


def get_video_md5(video_path, blocksize=2**20):
    m = hashlib.md5()
    try:
        with open(video_path, "rb") as file:
            while True:
                buf = file.read(blocksize)
                if not buf:
                    break
                m.update(buf)
        return m.hexdigest()
    except OSError:
        return "corrupt"
    except SyntaxError:
        return "corrupt"


# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


def main():
    imagecount = 0
    videocount = 0
    while True:
        start_time = time.process_time()
        if allfolders:
            workingdir = allfolders.pop(0)
            workingimages = listimages(workingdir)
            workingvideos = listvideos(workingdir)
            if subdiv.find("screenshots") != -1:
                is_screenshot = 1
                workingcollection = screenshotcollection
            elif subdiv.find("downloads") != -1:
                is_screenshot = 0
                workingcollection = downloadcollection
            elif subdiv.find("art") != -1:
                is_screenshot = 0
                workingcollection = artcollection
            else:
                is_screenshot = 0
                workingcollection = collection
            for imagepath in workingimages:
                im_md5 = get_md5(imagepath)
                relpath = os.path.relpath(imagepath, rootdir)

                tags_mongo = []
                while not tags_mongo:
                    try:
                        tags_mongo = workingcollection.find_one({"md5": im_md5}, {"vision_tags": 1, "_id": 0})
                        break
                    except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as e:
                        logger.warning("Connection error: %s", e)
                        time.sleep(10)
                tags = dumps(tags_mongo)

                text_mongo = []
                while not text_mongo:
                    try:
                        text_mongo = dumps(workingcollection.find_one({"md5": im_md5}, {"vision_text": 1, "_id": 0}))
                        break
                    except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as e:
                        logger.warning("Connection error: %s", e)
                        time.sleep(10)
                text = loads(text_mongo)

                logger.info("Processing image %s", imagepath)
                tagsjson = loads(tags)
                if tagsjson:
                    tags_list = tagsjson.get('vision_tags')
                    logger.info("Tags are %s", tags_list)
                    if tags_list:
                        exiftagger.write(imagepath, "Subject", tags_list)

                if text:
                    text_list = text.get('vision_text').replace('\n', '\\n')
                    logger.info("Text is %s", text_list)
                    if text_list:
                        exiftagger.write(imagepath, "xmp:Title", text_list)
                else:
                    logger.warning("No metadata found in MongoDB for image %s", imagepath)
                    imagecount += 1
            for videopath in workingvideos:
                is_screenshot = 0
                workingcollection = videocollection
                video_md5 = get_video_md5(videopath)
                tags = dumps(workingcollection.find_one({"md5": video_md5}, {"vision_tags": 1, "_id": 0}))
                text_array = loads(dumps(workingcollection.find_one({"md5": video_md5}, {"vision_text": 1, "_id": 0})))
                tagsjson = loads(tags)
                logger.info("Processing video %s", videopath)
                if tagsjson:
                    tags_list = tagsjson.get('vision_tags')
                    logger.info("Tags are %s", tags_list)
                    if tags_list:
                        logger.info("Writing text")
                        exiftagger.write(videopath, "Subject", tags_list)
                if text_array:
                    text_list = (' '.join(text_array.get('vision_text'))).replace('\n', '\\n')
                    logger.info("Text is %s", text_list)
                    if text_list:
                        logger.info("Writing text")
                        exiftagger.write(videopath, "Title", text_list)
                else:
                    logger.warning("No metadata found in MongoDB for video %s", videopath)
                    videocount += 1

        else:
            elapsed_time = time.process_time() - start_time
            final_time = str(datetime.timedelta(seconds=elapsed_time))
            logger.error("All entries processed. Root folder: %s Folder list: %s", rootdir, allfolders)
            print(imagecount, " media processed.")
            print("Processing took ", final_time)
            break


main()
