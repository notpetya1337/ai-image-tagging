import logging
import os
import sys
from configparser import ConfigParser
from tagging import Tagging
from mongoclient import get_database

from fileops import listdirs, get_image_md5, get_video_content_md5

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
        for document in list(workingcollection.find({'subdiv':  subdiv})):
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
        # TODO: add logic to process video entries as well
        break


if __name__ == "__main__":
    main()
