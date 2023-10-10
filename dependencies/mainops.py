import logging
import os
import sys

import pymongo

from dependencies.configops import MainConfig
from dependencies.fileops import (get_image_content, get_image_md5, get_video_content, get_video_content_md5,
                                  listimages, listvideos)
from dependencies.vision import Tagging
from dependencies.vision_video import VideoData

# TODO: add conditional import for deepb


# read config
config = MainConfig("config.ini")

# Initialize variables
tagging = Tagging(config.google_credentials, config.google_project, config.tags_backend)
# Names of likelihood from google.cloud.vision.enums
likelihood_name = ("UNKNOWN", "Very unlikely", "Unlikely", "Possible", "Likely", "Very likely")

# initialize DBs
currentdb = pymongo.MongoClient(config.connectstring)[config.mongodbname]
collection = currentdb[config.mongocollection]
screenshotcollection = currentdb[config.mongoscreenshotcollection]
videocollection = currentdb[config.mongovideocollection]


# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# TODO: turn most of this into a class


def create_imagedoc(
    image_content, im_md5, image_array, relpath_array, is_screenshot, subdiv
):
    text = tagging.get_text(image_binary=image_content)
    mongo_entry = {
        "md5": im_md5,
        "vision_text": text[0],
        "path": image_array,
        "subdiv": subdiv,
        "relativepath": relpath_array,
        "is_screenshot": is_screenshot,
    }
    if is_screenshot == 0:
        tags = tagging.get_tags(image_binary=image_content)
        safe = tagging.get_explicit(image_binary=image_content)
        mongo_entry.append = {
            "vision_tags": tags,
            "explicit_detection": [
                {
                    "adult": f"{likelihood_name[safe.adult]}",
                    "medical": f"{likelihood_name[safe.medical]}",
                    "spoofed": f"{likelihood_name[safe.spoof]}",
                    "violence": f"{likelihood_name[safe.violence]}",
                    "racy": f"{likelihood_name[safe.racy]}",
                }
            ],
        }
    logger.info("Generated MongoDB entry: %s", mongo_entry)
    return mongo_entry


def create_videodoc(
    video_content, video_content_md5, vidpath_array, relpath_array, subdiv
):
    videoobj = VideoData(config.google_credentials, config.google_project)
    videoobj.video_vision_all(video_content)
    mongo_entry = {
        "content_md5": video_content_md5,
        "vision_tags": videoobj.labels,
        "vision_text": videoobj.text,
        "vision_transcript": videoobj.transcripts,
        "explicit_detection": videoobj.pornography,
        "path": vidpath_array,
        "subdiv": subdiv,
        "relativepath": relpath_array,
    }
    return mongo_entry


def process_image(imagepath, workingcollection, subdiv, is_screenshot, rootdir=""):
    im_md5 = get_image_md5(imagepath)
    relpath = os.path.relpath(imagepath, rootdir)
    # if MD5 is not in MongoDB
    if workingcollection.find_one({"md5": im_md5}, {"md5": 1}) is None:
        image_content = get_image_content(imagepath)
        imagepath_array = [imagepath]
        relpath_array = [relpath]
        mongo_entry = create_imagedoc(
            image_content, im_md5, imagepath_array, relpath_array, is_screenshot, subdiv
        )
        workingcollection.insert_one(mongo_entry)
        # logger.info("Added new entry in MongoDB for image %s \n", imagepath)
        print("Added new entry in MongoDB for image %s \n", imagepath)
    # if MD5 is in MongoDB
    else:
        # if path is not in MongoDB
        if (
            workingcollection.find_one(
                {"md5": im_md5, "relativepath": relpath}, {"md5": 1, "relativepath": 1}
            )
            is None
        ):
            workingcollection.update_one(
                {"md5": im_md5},
                {"$addToSet": {"path": imagepath, "relativepath": relpath}},
            )
            logger.info("Added path in MongoDB for duplicate image %s", imagepath)
        # if path is in MongoDB
        else:
            logger.info("Image %s is in MongoDB", imagepath)


def process_video(videopath, subdiv, rootdir=""):
    video_content_md5 = str(get_video_content_md5(videopath))
    relpath = os.path.relpath(videopath, rootdir)
    # if content MD5 is not in Mongo
    if (
        videocollection.find_one({"content_md5": video_content_md5}, {"content_md5": 1})
        is None
    ):
        logger.info("Processing video %s", relpath)
        videopath_array = [videopath]
        video_content = get_video_content(videopath)
        relpath_array = [relpath]
        mongo_entry = create_videodoc(
            video_content, video_content_md5, videopath_array, relpath_array, subdiv
        )
        logger.info("Generated MongoDB entry: %s", mongo_entry)
        # noinspection PyUnresolvedReferences
        videocollection.insert_one(mongo_entry)
        logger.info("Added new entry in MongoDB for video %s \n", videopath)
    # if content MD5 is in MongoDB
    else:
        # if path is not in MongoDB
        if (
            videocollection.find_one(
                {"content_md5": video_content_md5, "relativepath": relpath},
                {"content_md5": 1, "relativepath": 1},
            )
            is None
        ):
            videocollection.update_one(
                {"content_md5": video_content_md5},
                {"$addToSet": {"path": videopath, "relativepath": relpath}},
            )
            logger.info("Added path in MongoDB for duplicate video %s", videopath)
        # if path is in MongoDB
        else:
            logger.info("Video %s is in MongoDB", videopath)


def process_image_folder(workingdir, workingcollection, is_screenshot, subdiv):
    # not recursive, must be called for each subfolder
    imagecount = 0
    rootdir = config.getdiv(subdiv)
    workingimages = listimages(workingdir, config.process_images)
    for imagepath in workingimages:
        imagecount += 1
        process_image(imagepath, workingcollection, subdiv, is_screenshot, rootdir)
    return imagecount


def process_video_folder(workingdir, subdiv):
    # not recursive, must be called for each subfolder
    videocount = 0
    rootdir = config.getdiv(subdiv)
    workingvideos = listvideos(workingdir, config.process_videos)
    for videopath in workingvideos:
        videocount += 1
        process_video(videopath, subdiv, rootdir)
    return videocount
