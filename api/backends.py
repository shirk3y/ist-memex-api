import happybase

import settings
from models import *

class AbstractBackend:
    def get(self, key):
        raise NotImplementedError
    def put(self, key, data, indices=[]):
        raise NotImplementedError
    def scan(self, prefix=None, start=None, stop=None, limit=None, expand=False):
        raise NotImplementedError
    def index(self, key):
        raise NotImplementedError

class HbaseLogBackend(AbstractBackend):
    def __init__(self):
        self.connection = happybase.Connection(host=settings.HBASE_HOST, port=int(settings.HBASE_PORT), table_prefix=settings.HBASE_TABLE_PREFIX)
    def get(self, key):
        row = self.connection.table('log').row(key)
        return row['f:vv']
    def put(self, key, data, indices=[]):
        self.connection.table('log').put(key, {'f:vv':data})
        for index in indices:
            kk = "{index_key}__{index_value}__{key}".format(index_key=index['key'], index_value=index['value'], key=key)
            self.index(kk)
        return self.get(key)
    def scan(self, prefix=None, start=None, stop=None, limit=None, expand=False):
        table = self.connection.table('index')
        keys = []
        if prefix:
            for key, data in table.scan(row_prefix=prefix, limit=limit):
                kk = key.split("__")[-1]
                keys.append(kk)
        elif start and stop:
            for key, data in table.scan(row_start=start, row_stop=stop, limit=limit):    
                kk = key.split("__")[-1]
                keys.append(kk)
        return keys
    def index(self, key):
        self.connection.table('index').put(key, {'f:vv':'1'})
        return key

class ModelLogBackend(AbstractBackend):
    def get(self, key):
        obj = Log.objects.get(key=key)
        return obj.data
    def put(self, key, data, indices=[]):
        obj,new = Log.objects.get_or_create(key=key)
        obj.data = data
        obj.save()
        for index in indices:
            kk = "{index_key}__{index_value}__{key}".format(index_key=index['key'], index_value=index['value'], key=key)
            self.index(kk)
        return self.get(key)
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
        obj, new = Index.objects.get_or_create(key=key)
        return obj.key
