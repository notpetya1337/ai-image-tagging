import logging
import os
import sys
from configparser import ConfigParser

from dependencies.fileops import listdirs, get_image_md5, get_video_content_md5
from dependencies.mongoclient import get_database
from dependencies.vision import Tagging

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
process_images = config.getboolean('storage', 'process_images')
process_videos = config.getboolean('storage', 'process_videos')

currentdb = get_database()
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
videocollection = currentdb[mongovideocollection]

videoextensions = (".mp4", ".webm", ".mov", ".mkv")
imageextensions = (".png", ".jpg", ".gif", ".jpeg")

if subdiv.find("screenshots") != -1:
    is_screenshot = 1
    workingcollection = screenshotcollection
else:
    is_screenshot = 0
    workingcollection = collection

rootdir = config.get('divs', 'cleanupdiv')

# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


def main():
    while True:
        if process_images:
            logger.info("Processing image DB %s", mongocollection)
            for document in list(workingcollection.find({'subdiv': subdiv})):
                for relpath in document["relativepath"]:
                    logger.info("Processing relative path")
                    docmd5 = document["md5"]
                    logger.info("Doc MD5 is %s", docmd5)
                    if relpath.endswith(imageextensions):
                        removepath = False
                        logger.info("Image path is %s", relpath)
                        fullpath = os.path.join(rootdir, relpath)
                        logger.info("Full path: %s", fullpath)
                        filefound = os.path.isfile(fullpath)
                        logger.info("File found status: %s", filefound)
                        if filefound:
                            pic_md5 = get_image_md5(os.path.join(rootdir, relpath))
                            logger.info("Image MD5 is %s", pic_md5)
                            if pic_md5 == docmd5:
                                logger.info("MD5 match for image %s", fullpath)
                            else:
                                logger.warning("MD5 mismatch for image file %s. Local hash is %s, MongoDB hash is %s",
                                               fullpath, pic_md5, docmd5)
                                removepath = True
                        elif not filefound:
                            removepath = True
                        if removepath:
                            logger.info("Pulling path record, {'_id': %s}, { '$pull': {'relativepath': { '$in': %s}}}",
                                        document["_id"], relpath)
                            workingcollection.update_one({'_id': document["_id"]},
                                                         {'$pull': {'relativepath': {'$in': [relpath]}}})
                            # pull relpath from current document by ID in Mongo

        if process_videos:
            logger.info("Processing video DB %s", mongovideocollection)
            for document in list(videocollection.find({'subdiv': subdiv})):
                for relpath in document["relativepath"]:
                    logger.info("Processing relative path")
                    docmd5 = document["content_md5"]
                    logger.info("Doc MD5 is %s", docmd5)
                    if relpath.endswith(videoextensions):
                        removepath = False
                        logger.info("Video path is %s", relpath)
                        fullpath = os.path.join(rootdir, relpath)
                        logger.info("Full path: %s", fullpath)
                        filefound = os.path.isfile(fullpath)
                        logger.info("File found status: %s", filefound)
                        if filefound:
                            vid_md5 = get_video_content_md5(os.path.join(rootdir, relpath))
                            logger.info("Video MD5 is %s", vid_md5)
                            if vid_md5 == docmd5:
                                logger.info("MD5 match for video %s", fullpath)
                            else:
                                logger.warning("MD5 mismatch for video file %s. Local hash is %s, MongoDB hash is %s",
                                               fullpath, vid_md5, docmd5)
                                removepath = True
                        elif not filefound:
                            removepath = True
                        if removepath:
                            logger.info("Pulling path record, {'_id': %s}, { '$pull': {'relativepath': { '$in': %s}}}",
                                        document["_id"], relpath)
                            videocollection.update_one({'_id': document["_id"]},
                                                       {'$pull': {'relativepath': {'$in': [relpath]}}})
                            # pull relpath from current document by ID in Mongo
        # TODO: add logic to process video entries as well
        break


if __name__ == "__main__":
    main()
