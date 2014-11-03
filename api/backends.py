import happybase

from models import *

class AbstractBackend:
    def get(self, key):
        raise NotImplementedError
    def put(self, key, data, indices=[]):
        raise NotImplementedError
    def scan(self, prefix=None, start=None, stop=None, limit=None):
        raise NotImplementedError
    def index(self, key):
        raise NotImplementedError

class HbaseBackend(AbstractBackend):
    def __init__(self):
        self.connection = happybase.Connection(host=settings.HBASE_HOST, port=settings.HBASE_PORT, table_prefix=settings.HBASE_TABLE_PREFIX)
    def get(self, key):
        raise NotImplementedError
    def put(self, key, data, indices=[]):
        raise NotImplementedError
    def scan(self, prefix=None, start=None, stop=None, limit=None):
        raise NotImplementedError
    def index(self, key):
        raise NotImplementedError

class ModelBackend(AbstractBackend):
    def get(self, key):
        obj = Log.objects.get(key=key)
        return obj.data
    def put(self, key, data, indices=[]):
        obj,new = Log.objects.get_or_create(key=key)
        obj.data = data
        obj.save()
        for index in indices:
            kk = "{index_key}__{index_value}__{key}".format(index_key=index['key'], index_value=index['value'], key=key)
            Index.objects.get_or_create(key=kk)
        return obj.data
    def scan(self, prefix=None, start=None, stop=None, limit=None, expand=False):
        keys = []
        if prefix:
            for obj in Index.objects.filter(key__startswith=prefix):
                key = obj.key.split("__")[-1]
                keys.append(key)
        elif start and stop:
            for obj in Index.objects.filter(key__gte=start, key__lte=stop):    
                key = obj.key.split("__")[-1]
                keys.append(key)
        return keys
    def index(self, key):
        obj = Index.objects.get_or_create(key=key)
        return obj.key
