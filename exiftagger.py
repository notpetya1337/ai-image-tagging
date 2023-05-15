from pyexif import pyexif
import logging
import sys
from pathlib import Path
from configparser import ConfigParser
import shutil

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

# initialize config
config = ConfigParser()
config.read('config.ini')
trashpath = config.get('divs', 'trashfolder')

# TODO: currently breaks with following error on .png files:
# Warning: [minor] Ignored empty rdf:Bag list for Iptc4xmpExt:LocationCreated
# Error: [minor] IFD0 pointer references previous IFD0 directory


def read(path, tag):
    metadata = pyexif.ExifEditor(path)
    try:
        return metadata.getTag(tag)
    except RuntimeError as e:
        logger.error("Error reading tags from image %s with error %s", path, e)


def write(path, tag, data):
    metadata = pyexif.ExifEditor(path, extra_opts="-P -ec")
    try:
        metadata.setTag(tag, data)
    except RuntimeError as e:
        logger.error("Error writing tags to image %s with error %s", path, e)
        if "Not a valid PNG (looks more like a JPEG)" in str(e):
            logger.warning("Renaming image %s to .jpg", path)
            libpath = Path(path)
            try:
                libpath.rename(libpath.with_suffix('.jpg'))
            except FileExistsError as ee:
                logger.warning("File already exists, moving to trash folder. Rename failed with error %s. ", ee)
                newfilename = (libpath.with_suffix('.jpg')).name
                try:
                    shutil.move(path, trashpath + newfilename)
                except Exception as eee:
                    logger.error("Exception %s moving image", eee)
        elif "Not a valid JPG (looks more like a PNG)" in str(e):
            logger.warning("Renaming image %s to .png", path)
            libpath = Path(path)
            try:
                libpath.rename(libpath.with_suffix('.png'))
            except FileExistsError as ee:
                logger.warning("File already exists, moving to trash folder. Rename failed with error %s. ", ee)
                newfilename = (libpath.with_suffix('.png')).name
                shutil.move(path, trashpath + newfilename)
        else:
            logger.error("RuntimeException %s not handled", e)
    except UnicodeDecodeError as e:
        logger.error("Error decoding image %s with error %s", path, e)
    except FileNotFoundError as e:
        logger.error("Mucho texto (FileNotFound, most likely too much text) for image %s with error %s and tags %s",
                     path, e, tag)


def update(path, tag, data):
    metadata = pyexif.ExifEditor(path, extra_opts="-P -ec")
    internal = metadata.getTag(tag)
    if internal is not None and (data in internal):
        return
    if internal:
        internal.append(data)
        metadata.setTag(tag, internal)
    else:
        logger.warning("No tags for %s", path)
