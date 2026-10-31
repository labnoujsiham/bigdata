import os
from dotenv import load_dotenv

load_dotenv()


def read_setting(name):
    if name in os.environ:
        return os.environ[name]
    raise RuntimeError("Missing setting: " + name + " (check your .env file)")


MONGO_URI = read_setting("MONGO_URI")
MONGO_DB = read_setting("MONGO_DB")
SPARK_MASTER = read_setting("SPARK_MASTER")