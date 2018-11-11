#!/usr/bin/python
"""
resetMDB.py
"""

from mongoBall import mongoBall

mdb = mongoBall()

mdb_collection_name = "test"

mdb_collection = mdb.collection(mdb_collection_name)

mdb_collection.delete_many({})
