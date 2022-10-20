from pyexif import pyexif


def read(path, tag):
    metadata = pyexif.ExifEditor(path)
    return metadata.getTag(tag)


def write(path, tag, data):
    metadata = pyexif.ExifEditor(path)
    metadata.setTag(tag, data)


def update(path, tag, data):
    metadata = pyexif.ExifEditor(path)
    internal = metadata.getTags(tag)
    internal.append(data)
    metadata.setTag(tag, internal)
