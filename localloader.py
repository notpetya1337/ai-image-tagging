import os

rootdir = './'
# TODO: load from config


def listdirs(folder):
    for result in os.scandir(folder):
        if result.is_dir():
            print(result.path)
            listdirs(result)


listdirs(rootdir)
