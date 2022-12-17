# Google Cloud tagging for images

## Functionality:
This script takes images from a local folder, processes the images through Google's Vision AI, retrieves AI-generated labels and OCRed text, and writes it to EXIF tags.

Tags and lightly OCRed text are written to a SQLite DB that uses image MD5 as the unique key.\
Google Vision currently allows 1000 free requests per month and charges $1.50 per 1000 requests afterward.

## Use case:
The Google Photos app allows you to search based on the EXIF "ImageDescription" field. You can also use ElasticSearch as an image search backend, possibly with a custom app to show image previews.

## TODO:
Make app run as service and automatically process new uploads\
Add support for DeepDetect (locally hosted image recognition)\
Add relative image location to ElasticSearch tags\
Store image MD5 to ElasticSearch\
Add script to update image locations in ElasticSearch based on MD5\
Add OCR tagging\
Add support for AWS Rekognition\
Add better validation for returned OCR text (currently, all characters matching [^ -~] are stripped)\
Add batch processing and/or multithreading (processing a single image currently takes 1-3 seconds)\
Possibly add web GUI