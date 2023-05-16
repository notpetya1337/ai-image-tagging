import logging
import sys
import time
import datetime
from configparser import ConfigParser
from tagging import Tagging
from mongoclient import get_database
import pymongo
from bson.json_util import dumps, loads
import exiftagger
from fileops import listdirs, listimages, listvideos, get_image_md5, get_video_content_md5

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
mongocollection = config.get('storage', 'mongocollection')
mongoscreenshotcollection = config.get('storage', 'mongoscreenshotcollection')
mongovideocollection = config.get('storage', 'mongovideocollection')
process_videos = config.getboolean('storage', 'process_videos')
process_images = config.getboolean('storage', 'process_images')
check_mongo = config.getboolean('storage', 'check_mongo')

# initialize DBs
currentdb = get_database()
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
videocollection = currentdb[mongovideocollection]

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
            workingimages = listimages(workingdir, process_images)
            workingvideos = listvideos(workingdir, process_videos)
            workingcollection = collection
            for imagepath in workingimages:
                im_md5 = get_image_md5(imagepath)
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
                    # noinspection PyUnresolvedReferences
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
                workingcollection = videocollection
                video_content_md5 = get_video_content_md5(videopath)
                # noinspection PyUnresolvedReferences
                try:
                    tags = dumps(workingcollection.find_one({"content_md5": video_content_md5},
                                                            {"vision_tags": 1, "_id": 0}))
                except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as e:
                    logger.warning("Connection error: %s", e)
                    time.sleep(10)
                    continue
                text_array = loads(dumps(workingcollection.find_one({"md5": video_content_md5},
                                                                    {"vision_text": 1, "_id": 0})))
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
                    logger.warning("No metadata found in MongoDB for video %s, md5 %s", videopath, video_content_md5)
                    videocount += 1

        else:
            elapsed_time = time.process_time() - start_time
            final_time = str(datetime.timedelta(seconds=elapsed_time))
            logger.error("All entries processed. Root folder: %s Folder list: %s", rootdir, allfolders)
            print(imagecount, " media processed.")
            print("Processing took ", final_time)
            break


main()
