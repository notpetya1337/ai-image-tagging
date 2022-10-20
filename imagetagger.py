from pyexif import pyexif

# TODO: currently breaks with following error on .png files:
# Warning: [minor] Ignored empty rdf:Bag list for Iptc4xmpExt:LocationCreated
# Error: [minor] IFD0 pointer references previous IFD0 directory

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
