import os
import logging
import sys
import io
import hashlib
import subprocess
import re
from PIL import Image
from shellescape import quote

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)


# list all subdirectories in a given folder
def listdirs(folder):
    internallist = [folder]
    for root, directories, files in os.walk(folder):
        for directory in directories:
            internallist.append(os.path.join(root, directory))
    return internallist


# list all images in a given folder
def listimages(subfolder, process_images):
    imageextensions = (".png", ".jpg", ".gif", ".jpeg", ".webp")
    internallist = []
    if not process_images:
        logger.warning("Not processing images")
        return internallist
    for file in os.listdir(subfolder):
        if file.endswith(imageextensions):
            imagepath = os.path.join(subfolder, file)
            internallist.append(imagepath)
    return internallist


def listvideos(subfolder, process_videos):
    videoextensions = (".mp4", ".webm", ".mov", ".mkv")
    internallist = []
    if not process_videos:
        logger.warning("Not processing videos")
        return internallist
    for file in os.listdir(subfolder):
        if file.endswith(videoextensions):
            videopath = os.path.join(subfolder, file)
            internallist.append(videopath)
    return internallist


# open an image at a given path
def get_image_content(image_path):
    with io.open(image_path, 'rb') as image:
        return image.read()


# open a video at a given path
def get_video_content(video_path):
    with io.open(video_path, 'rb') as video:
        return video.read()


def get_image_md5(image_path):
    try:
        with Image.open(image_path) as im:
            return hashlib.md5(im.tobytes()).hexdigest()
    except OSError:
        return "corrupt"
    except SyntaxError:
        return "corrupt"


def get_video_content_md5(video_path):
    try:
        #process = subprocess.Popen('bash -c \'ffmpeg -i "{vpath}" -map 0:v -f md5 -\''.format(vpath=video_path),
        #                           shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process = subprocess.Popen('cmd /c ffmpeg.exe -i "{vpath}" -map 0:v -f md5 -'.format(vpath=video_path),
                                   shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        md5list = re.findall(r"MD5=([a-fA-F\d]{32})", str(out))
        logger.info("Got content MD5 for video %s: %s", video_path, md5list)
        md5 = md5list[0]
    except Exception as e:
        logger.error("Unhandled exception getting MD5 for path %s with ffmpeg: %s", video_path, e)
        md5 = "corrupt"
    return md5
