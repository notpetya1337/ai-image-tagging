import os

rootdir = './'
# TODO: load from config
def listdirs(rootdir):
    for it in os.scandir(rootdir):
        if it.is_dir():
            print(it.path)
            listdirs(it)
listdirs(rootdir)