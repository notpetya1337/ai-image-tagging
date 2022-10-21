import logging
import os
import sys
from configparser import ConfigParser

import imagetagger
from tagging import Tagging

# logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.debug("logging started")
logger = logging.getLogger(__name__)

config = ConfigParser()
config.read('config.ini')
rootdir = config.get('storage', 'localimagedir')


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
    for file in os.listdir(subfolder):
        if file.endswith(imageextensions):
            imagepath = os.path.join(subfolder, file)
            internallist.append(imagepath)
    return internallist


# open an image at a given path
def get_image_content(image_path):
    image = open(image_path, 'rb')
    return image.read()


# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


def main():
    while True:
        tags = []
        if allfolders:
            workingdir = allfolders.pop(0)
            workingimages = listimages(workingdir)
            for image in workingimages:
                if imagetagger.read(image, "UniqueCameraModel") == 1234:
                    logging.debug("=%s already processed", image)
                    continue
                    # skips image if exif.read returns 1234
                else:
                    logger.info('opening image=%s', image)
                    image_content = get_image_content(image)
                    logger.info('getting tags for image=%s', image)
                    tags = tagging.get_tags(image_binary=image_content)
                    text = tagging.get_text(image_binary=image_content)
                    print(image, tags, text)
                    imagetagger.update(image, "ImageDescription", tags)
                    imagetagger.update(image, "UserComment", text)
                    imagetagger.write(image, "UniqueCameraModel", "1234")
                    logger.info('wrote EXIF data for image=%s', image)
                    print("EXIF results: ", imagetagger.read(image, "ImageDescription"),
                          imagetagger.read(image, "UserComment"),
                          imagetagger.read(image, "UniqueCameraModel"))
        else:
            print("No folders found", rootdir, allfolders)
            break


main()
