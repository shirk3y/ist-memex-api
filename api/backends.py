from models import *

class AbstractBackend:
    def get(self, key):
        raise NotImplementedError
    def put(self, key, data):
        raise NotImplementedError
    def scan(self, prefix=None, start=None, stop=None, limit=None):
        raise NotImplementedError
    def index(self, key):
        raise NotImplementedError

class HbaseBackend(AbstractBackend):
    pass

class ModelBackend(AbstractBackend):
    def get(self, key):
        obj = Log.objects.get(key=key)
        return obj.data
    def put(self, key, data):
        obj = Log.objects.create(key=key, data=data)
        return obj.data
    def scan(self, prefix=None, start=None, stop=None, limit=None):
        raise NotImplementedError
    def index(self, key):
        raise NotImplementedError
