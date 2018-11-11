import pymongo
from pymongo import MongoClient
import pprint

client = MongoClient("mongodb://localhost:27017/")

test_s = {
    "key1" : "test",
    "key2" : "apple"
}

db = client.exampleDB
db.test.insert(test_s);

for a in db.test.find():
    pprint.pprint(a)