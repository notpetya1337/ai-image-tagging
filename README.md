# Google Cloud tagging for images

## Functionality:

main.py takes images from a local folder, checks the MD5 against a MongoDB database, processes the images through
Google's Vision AI, retrieves AI-generated labels and OCRed text, and writes the data to MongoDB documents.\
tagwriter.py reads the data from MongoDB and writes it to EXIF tags.\
cleanup.py checks the paths in MongoDB against local files and removes any path entries that don't exist locally.\
Google Vision currently allows 1000 free requests per month and charges $1.50 per 1000 requests afterward.

## Use case:

Windows Explorer indexes EXIF tags. On Android, the Aves app allows you to search by tags and description text in EXIF.

## TODO:

Make app run as service and automatically process new files\
Add support for DeepDetect (locally hosted image recognition)\
Add support for AWS Rekognition

## Requirements:

To be able to process videos, ffmpeg needs be installed and accessible.\
Copy config-example.ini to config.ini and set variables in the config.
At minimum, you need MongoDB credentials, GCP credentials, and a local folder of images to process.
