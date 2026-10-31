import os
import sys
import time

# this file is 3 levels below the repo root: queries/siham_queries/common.py
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from pymongo import MongoClient

import config

client = MongoClient(config.MONGO_URI)
db = client[config.MONGO_DB]
client.admin.command("ping")   # connect first, so the connection time is not counted


def timed(label, func, *args):
    start = time.perf_counter()
    result = func(*args)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print("%s: %.2f ms" % (label, elapsed_ms))
    return result