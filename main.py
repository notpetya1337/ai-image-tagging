import logging
import os
import sys
from configparser import ConfigParser

import imagetagger
from tagging import Tagging
import sqlite3

# import Image
from PIL import Image
import hashlib

# logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.debug("logging started")
logger = logging.getLogger(__name__)

config = ConfigParser()
config.read('config.ini')
rootdir = config.get('storage', 'localimagedir')
sqldb = config.get('storage', 'sqlitedb')
subdiv = config.get('properties', 'subdiv')

# initialize DB
con = sqlite3.connect(sqldb)
cur = con.cursor()


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


def get_md5(image_path):
    im = Image.open(image_path)
    return hashlib.md5(im.tobytes()).hexdigest()


# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


def main():
    # cur.execute("CREATE TABLE IF NOT EXISTS TABLE_NAME (column_name datatype, column_name datatype);")
    cur.execute("""CREATE TABLE IF NOT EXISTS media (md5 TEXT NOT NULL PRIMARY KEY, vision_tags TEXT, vision_ocr TEXT, path TEXT, is_screenshot BOOLEAN NOT NULL CHECK (is_screenshot IN (0, 1)), subdiv TEXT);""")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS screenshots (md5 INTEGER NOT NULL PRIMARY KEY, vision_text TEXT, names TEXT);")
    logger.info('DB initialized')
    # except:
    #     logger.error('Unable to initialize DB')
    while True:
        tags = []
        if allfolders:
            workingdir = allfolders.pop(0)
            workingimages = listimages(workingdir)
            for image in workingimages:
                # for reference, image is always going to be an absolute path
                im_md5 = get_md5(image)
                pathselect = cur.execute("SELECT path FROM media WHERE md5=?", (im_md5,))
                check = pathselect.fetchone()
                if check is None:
                    # check file path for "screenshot"
                    is_screenshot = 0
                    logger.info('=%s is not in database', image)
                    logger.info('opening image=%s', image)
                    image_content = get_image_content(image)
                    logger.info('getting tags for image=%s', image)
                    tags = tagging.get_tags(image_binary=image_content)
                    text = tagging.get_text(image_binary=image_content)
                    str_tags = ' '.join(tags)
                    print(image, str_tags, text)
                    cur.execute("INSERT INTO media VALUES (?,?,?,?,?,?)", (im_md5, str_tags, text, image, is_screenshot, subdiv))
                    con.commit()
                else:
                    # add a path entry
                    logger.info('Todo, sql result is =%s', check)



                # if imagetagger.read(image, "UniqueCameraModel") == 1234:
                #     logging.debug("=%s already processed", image)
                #     continue
                #     # skips image if exif.read returns 1234

                    # imagetagger.update(image, "ImageDescription", tags)
                    # imagetagger.update(image, "UserComment", text)
                    # imagetagger.write(image, "UniqueCameraModel", "1234")
                    # logger.info('wrote EXIF data for image=%s', image)
                    # print("EXIF results: ", imagetagger.read(image, "ImageDescription"),
                    #       imagetagger.read(image, "UserComment"),
                    #       imagetagger.read(image, "UniqueCameraModel"))

        else:
            print("No folders found", rootdir, allfolders)
            break


main()
