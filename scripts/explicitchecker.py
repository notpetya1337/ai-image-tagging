import datetime
import logging
import os
import sys
import time
from configparser import ConfigParser

import pymongo

from dependencies.fileops import listdirs, listimages, listvideos, \
    get_image_md5, get_image_content, get_video_content, get_video_content_md5
from dependencies.mongoclient import get_database
from dependencies.vision import Tagging
from dependencies.vision_video import VideoData

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

# read config
config = ConfigParser()
config.read('config.ini')
subdiv = config.get('properties', 'subdiv')
rootdir = config.get('divs', subdiv)
mongocollection = config.get('storage', 'mongocollection')
mongoscreenshotcollection = config.get('storage', 'mongoscreenshotcollection')
mongovideocollection = config.get('storage', 'mongovideocollection')
process_videos = config.getboolean('storage', 'process_videos')
process_images = config.getboolean('storage', 'process_images')

# initialize DBs
currentdb = get_database()
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
videocollection = currentdb[mongovideocollection]
collection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
collection.create_index('vision_tags')
screenshotcollection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
videocollection.create_index([('content_md5', pymongo.TEXT)], name='content_md5_index')
videocollection.create_index('md5')
videocollection.create_index('vision_tags')

# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)

# Names of likelihood from google.cloud.vision.enums
likelihood_name = (
    "UNKNOWN",
    "Very unlikely",
    "Unlikely",
    "Possible",
    "Likely",
    "Very likely",
)


def main():
    while True:
        imagecount = 0
        videocount = 0
        start_time = time.time()
        if allfolders:
            workingdir = allfolders.pop(0)
            workingimages = listimages(workingdir, process_images)
            workingvideos = listvideos(workingdir, process_videos)
            for imagepath in workingimages:
                imagecount += 1
                if subdiv.find("screenshots") != -1:
                    is_screenshot = 1
                    workingcollection = screenshotcollection
                else:
                    is_screenshot = 0
                    workingcollection = collection
                im_md5 = get_image_md5(imagepath)
                # if MD5 is in MongoDB and explicit tags aren't:
                if workingcollection.find_one({"$and": [{"md5": im_md5}, {"explicit_detection": {"$exists": False}}]}):
                    image_content = get_image_content(imagepath)
                    # noinspection PyUnresolvedReferences
                    try:
                        safe = tagging.get_explicit(image_binary=image_content)
                        workingcollection.update_one({"md5": im_md5},
                                                     {"$set": {"explicit_detection": [
                                                         {"adult": f"{likelihood_name[safe.adult]}",
                                                          "medical": f"{likelihood_name[safe.medical]}",
                                                          "spoofed": f"{likelihood_name[safe.spoof]}",
                                                          "violence": f"{likelihood_name[safe.violence]}",
                                                          "racy": f"{likelihood_name[safe.racy]}"}]}})
                    except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as e:
                        logger.warning("Connection error: %s", e)
                        time.sleep(10)
                    logger.info("Added explicit tags in MongoDB for image %s \n", imagepath)
                    continue
                else:
                    logger.info("Image %s is in MongoDB", imagepath)
                    continue
            for videopath in workingvideos:
                videocount += 1
                workingcollection = videocollection
                video_content_md5 = str(get_video_content_md5(videopath))
                relpath = os.path.relpath(videopath, rootdir)
                # if MD5 is in MongoDB and explicit tags aren't:
                if workingcollection.find_one(
                        {"$and": [{"content_md5": video_content_md5}, {"explicit_detection": {"$exists": False}}]}):
                    # noinspection PyUnresolvedReferences
                    try:
                        logger.info("Processing video %s", relpath)
                        video_content = get_video_content(videopath)
                        videoobj = VideoData()
                        videoobj.video_vision_explicit(video_content)
                        workingcollection.update_one({"content_md5": video_content_md5},
                                                     {"$set": {"explicit_detection": videoobj.pornography}})
                    except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as e:
                        logger.warning("Connection error: %s", e)
                        time.sleep(10)
                    logger.info("Added explicit tags in MongoDB for video %s \n", videopath)
        else:
            elapsed_time = time.time() - start_time
            final_time = str(datetime.timedelta(seconds=elapsed_time))
            logger.info("All entries processed. Root folder: %s Folder list: %s", rootdir, allfolders)
            print(imagecount, "images and ", videocount, "processed.")
            print("Processing took ", final_time)
            break


if __name__ == "__main__":
    main()
