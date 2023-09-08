import datetime
import logging
import sys
import time
from configparser import ConfigParser

import exiftool
import pymongo
from bson.json_util import dumps, loads

from dependencies.fileops import (get_image_md5, get_video_content_md5, listdirs, listimages, listvideos)
from dependencies.vision import Tagging

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger("PIL").setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

config = ConfigParser()
config.read("config.ini")

# Tags go to Subject, comma separated
# Text goes to Description for now


# read config
config = ConfigParser()
config.read("config.ini")
subdiv = config.get("properties", "subdiv")
rootdir = config.get("divs", subdiv)
connectstring = config.get('storage', 'connectionstring')
mongodbname = config.get('storage', 'mongodbname')
mongocollection = config.get("storage", "mongocollection")
mongoscreenshotcollection = config.get("storage", "mongoscreenshotcollection")
mongovideocollection = config.get("storage", "mongovideocollection")
process_videos = config.getboolean("storage", "process_videos")
process_images = config.getboolean("storage", "process_images")

# initialize DBs
currentdb = pymongo.MongoClient(connectstring)[mongodbname]
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
videocollection = currentdb[mongovideocollection]

# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


def main():
    et = exiftool.ExifToolHelper(
        logger=logging.getLogger(__name__).setLevel(logging.INFO), encoding="utf-8"
    )
    imagecount = 0
    videocount = 0
    start_time = time.time()
    if subdiv.find("screenshots") != -1:
        is_screenshot = 1
        workingcollection = screenshotcollection
    else:
        is_screenshot = 0
        workingcollection = collection
    while True:
        if allfolders:
            workingdir = allfolders.pop(0)
            workingimages = listimages(workingdir, process_images)
            workingvideos = listvideos(workingdir, process_videos)
            if subdiv.find("screenshots") != -1:
                workingcollection = screenshotcollection
            else:
                workingcollection = collection
            for imagepath in workingimages:
                imagecount += 1
                im_md5 = get_image_md5(imagepath)
                tags_mongo = []
                while not tags_mongo:
                    # noinspection PyUnresolvedReferences
                    try:
                        tags_mongo = workingcollection.find_one(
                            {"md5": im_md5}, {"vision_tags": 1, "_id": 0}
                        )
                        break
                    except (
                        pymongo.errors.ServerSelectionTimeoutError,
                        pymongo.errors.AutoReconnect,
                    ) as e:
                        logger.warning("Connection error: %s", e)
                        time.sleep(10)
                tags = dumps(tags_mongo)
                text_mongo = []
                while not text_mongo:
                    # noinspection PyUnresolvedReferences
                    try:
                        text_mongo = dumps(
                            workingcollection.find_one(
                                {"md5": im_md5}, {"vision_text": 1, "_id": 0}
                            )
                        )
                        break
                    except (
                        pymongo.errors.ServerSelectionTimeoutError,
                        pymongo.errors.AutoReconnect,
                    ) as e:
                        logger.warning("Connection error: %s", e)
                        time.sleep(10)
                text = loads(text_mongo)

                explicit_mongo = []
                while not explicit_mongo:
                    if is_screenshot == 1:
                        detection_results = []
                    else:
                        # noinspection PyUnresolvedReferences
                        try:
                            explicit_mongo = workingcollection.find_one(
                                {"md5": im_md5}, {"explicit_detection": 1, "_id": 0}
                            )
                            detobj = explicit_mongo["explicit_detection"]
                            detobj = detobj[0]
                            detection_results = (
                                f"Adult: {detobj['adult']}",
                                f"Medical: {detobj['medical']}",
                                f"Spoofed: {detobj['spoofed']}",
                                f"Violence: {detobj['violence']}",
                                f"Racy: {detobj['racy']}",
                            )
                            break
                        except (
                            pymongo.errors.ServerSelectionTimeoutError,
                            pymongo.errors.AutoReconnect,
                        ) as e:
                            logger.warning("Connection error: %s", e)
                            time.sleep(10)
                        except KeyError as e:
                            logger.warning("Explicit tags not found for %s", imagepath)
                            detection_results = []

                logger.info("Processing image %s", imagepath)
                tagsjson = loads(tags)
                tagsjson["vision_tags"].extend(detection_results)
                if tagsjson:
                    tags_list = tagsjson.get("vision_tags")
                    logger.info("Tags are %s", tags_list)
                    if tags_list:
                        try:
                            et.set_tags(
                                imagepath,
                                tags={"Subject": tags_list},
                                params=["-P", "-overwrite_original"],
                            )
                        except exiftool.exceptions.ExifToolExecuteError as e:
                            logger.warning('Error: "%s " while writing tags', e)

                if text:
                    text_list = text.get("vision_text").replace("\n", "\\n")
                    logger.info("Text is %s", text_list)
                    if text_list:
                        try:
                            et.set_tags(
                                imagepath,
                                tags={"xmp:Title": text_list},
                                params=["-P", "-overwrite_original", "-ec"],
                            )
                        except exiftool.exceptions.ExifToolExecuteError as e:
                            logger.warning("Error %s writing tags", e)
                else:
                    logger.warning(
                        "No metadata found in MongoDB for image %s", imagepath
                    )
            for videopath in workingvideos:
                videocount += 1
                workingcollection = videocollection
                video_content_md5 = get_video_content_md5(videopath)
                # noinspection PyUnresolvedReferences
                try:
                    tags = dumps(
                        workingcollection.find_one(
                            {"content_md5": video_content_md5},
                            {"vision_tags": 1, "_id": 0},
                        )
                    )
                except (
                    pymongo.errors.ServerSelectionTimeoutError,
                    pymongo.errors.AutoReconnect,
                ) as e:
                    logger.warning("Connection error: %s", e)
                    time.sleep(10)
                    continue
                text_array = loads(
                    dumps(
                        workingcollection.find_one(
                            {"content_md5": video_content_md5},
                            {"vision_text": 1, "_id": 0},
                        )
                    )
                )

                explicit_mongo = []
                while not explicit_mongo:
                    # noinspection PyUnresolvedReferences
                    try:
                        explicit_mongo = workingcollection.find_one(
                            {"content_md5": video_content_md5},
                            {"explicit_detection": 1, "_id": 0},
                        )
                        detobj = explicit_mongo["explicit_detection"]
                        break
                    except (
                        pymongo.errors.ServerSelectionTimeoutError,
                        pymongo.errors.AutoReconnect,
                    ) as e:
                        logger.warning("Connection error: %s", e)
                        time.sleep(10)
                    except KeyError as e:
                        logger.warning("Explicit tags not found for %s", videopath)
                        detobj = []
                tagsjson = loads(tags)
                tagsjson["vision_tags"].extend(detobj)
                logger.info("Processing video %s", videopath)
                if tagsjson:
                    tags_list = tagsjson.get("vision_tags")
                    logger.info("Tags are %s", tags_list)
                    if tags_list:
                        logger.info("Writing text")
                        try:
                            et.set_tags(
                                videopath,
                                tags={"Subject": tags_list},
                                params=["-P", "-overwrite_original"],
                            )
                        except exiftool.exceptions.ExifToolExecuteError as e:
                            logger.warning('Error: "%s " while writing tags', e)
                if text_array:
                    text_list = (" ".join(text_array.get("vision_text"))).replace(
                        "\n", "\\n"
                    )
                    logger.info("Text is %s", text_list)
                    if text_list:
                        logger.info("Writing text")
                        try:
                            et.set_tags(
                                videopath,
                                tags={"Title": text_list},
                                params=["-P", "-overwrite_original", "-ec"],
                            )
                        except exiftool.exceptions.ExifToolExecuteError as e:
                            logger.warning('Error: "%s " while writing tags', e)
                else:
                    logger.warning(
                        "No metadata found in MongoDB for video %s, md5 %s",
                        videopath,
                        video_content_md5,
                    )

        else:
            elapsed_time = time.time() - start_time
            final_time = str(datetime.timedelta(seconds=elapsed_time))
            logger.info(
                "All entries processed. Root folder: %s Folder list: %s",
                rootdir,
                allfolders,
            )
            print(imagecount, "images and ", videocount, "videos processed.")
            print("Processing took ", final_time)
            et.terminate()
            break


main()
