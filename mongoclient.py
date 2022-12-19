from pymongo import MongoClient
from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')
connectstring = config.get('storage', 'connectionstring')
mongodbname = config.get('storage', 'mongodbname')


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    # CONNECTION_STRING = "mongodb+srv://user:pass@cluster.mongodb.net/myFirstDatabase"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(connectstring)

    # Create the database for our example
    return client[mongodbname]


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
    # Get the database
    dbname = get_database()
