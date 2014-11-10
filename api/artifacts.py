import sys
import time
import re
import happybase
import jsonschema

from django.core.exceptions import ValidationError
from django.utils.text import slugify
from rest_framework.views import APIView
from rest_framework.response import Response

import settings

from models import *
from brokers import GenericRecordBroker
from backends import AbstractBackend

class ArtifactList(APIView):
    def get(self, request, format=None):
        limit = int(request.QUERY_PARAMS.get('limit', 100))
        expand = bool(request.QUERY_PARAMS.get('expand', False))
        broker = ArtifactBroker(settings.API_ARTIFACT_MANAGER_BACKEND)
        response = broker.search(index='timestamp',prefix='', limit=limit, expand=expand)
        return Response(response)
    def post(self, request, format=None):
        broker = ArtifactBroker(settings.API_ARTIFACT_MANAGER_BACKEND)
        response = broker.save(request.DATA)
        return Response(response)

class ArtifactBroker(GenericRecordBroker):

    SCHEMA = { 
        "type": "object",
        "properties": {
            "key": { 
                "type": "string", 
            },
            "url": {
                "type": "string", 
            },
            "timestamp": {
                "type": "number",
                "minimum": 1000000000000,
                "maximum": 9999999999999,
                "multipleOf": 1.0,
            },
            "request": {
                "type": "object",
            },
            "response": {
                "type": "object",
            },
        },
    }

    MAX_OBJECT_SIZE = 5 * 1024 * 1024 #5MB

    def get(self, key):
        data = self.backend.get(key)
        doc = self.deserialize(data)
        doc = self.strip_indices(doc)
        return doc

    def save(self, doc, key = None):
        doc, key = self.validate(doc, key)
        data = self.serialize(doc)
        dataSize = sys.getsizeof(data)
        if dataSize > ArtifactBroker.MAX_OBJECT_SIZE:
            raise ValidationError("Object data size ({dataSize} bytes) exceeds max object size ({maxSize} bytes)".format(dataSize=dataSize, maxSize=ArtifactBroker.MAX_OBJECT_SIZE))
        self.backend.put(key, data, doc['indices'])
        return self.get(key)

    def search(self, index, value=None, prefix=None, start=None, end=None, limit=None, expand=False):
        if index in ('timestamp'):
            if value:
                try:
                    value = self.flip_timestamp(value)
                except:
                    pass
            if prefix:
                try:
                    prefix = self.flip_timestamp(prefix)
                except:
                    pass
            if start and end:
                _end = self.flip_timestamp(start)
                _start = self.flip_timestamp(end)
                end = str(int(_end)+1)
                start = _start
        return GenericRecordBroker.search(self, index, value, prefix, start, end, limit, expand)

    def validate(self, doc, key = None):

        doc['indices'] = doc.get('indices', [])

        doc, key = self.validate_key(doc, key)
        jsonschema.validate(doc, ArtifactBroker.SCHEMA)
        return doc, key

    def validate_key(self, doc, key = None):
        _key = doc.get('key', '').lower()
        # first make sure we have a key; if not, generate a random key
        if not key:
            if _key:
                key = _key
            else:
                key = _key = str(uuid.uuid4())
        key = key.lower()
        # now make sure the two match
        if key and _key and key != _key:
            raise ValidationError("Key mismatch; '{key1}' != '{key2}'".format(key1=key, key2=_key))
        # finally, check that the key is in the correct format
        if not bool(re.compile('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}').match(key)):
            raise ValidationError("Invalid key: '{key}'".format(key=key))
        doc['key'] = key
        return doc, key

    def validate_timestamp(self, ts):
        try:
            return int(ts)
        except:
            raise ValidationError("Parser error: '{ts}' could not interpreted as a timestamp".format(ts=ts))

    def flip_timestamp(self, ts):
        flipped = []
        for cc in str(ts):
            flipped.append(str(9 - int(cc)))
        return "".join(flipped)
    
    def add_index(self, doc, key, value):
        try:
            self.validate_index(key, value)
            doc['indices'].append({'key':key, 'value':value})
        except ValidationError:
            pass

class HbaseArtifactBackend(AbstractBackend):
    def __init__(self):
        self.connection = happybase.Connection(host=settings.HBASE_HOST, port=int(settings.HBASE_PORT), table_prefix=settings.HBASE_TABLE_PREFIX)
    def get(self, key):
        row = self.connection.table('artifact').row(key)
        return row['f:vv']
    def put(self, key, data, indices=[]):
        self.connection.table('artifact').put(key, {'f:vv':data})
        for index in indices:
            kk = "{index_key}__{index_value}__{key}".format(index_key=index['key'], index_value=index['value'], key=key)
            self.index(kk)
        return self.get(key)
    def scan(self, prefix=None, start=None, stop=None, limit=None, expand=False):
        table = self.connection.table('artifact_index')
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
        self.connection.table('artifact_index').put(key, {'f:vv':'1'})
        return key

class ModelArtifactBackend(AbstractBackend):
    def get(self, key):
        obj = Artifact.objects.get(key=key)
        return obj.data
    def put(self, key, data, indices=[]):
        obj,new = Artifact.objects.get_or_create(key=key)
        obj.data = data
        obj.save()
        for index in indices:
            kk = "{index_key}__{index_value}__{key}".format(index_key=index['key'], index_value=index['value'], key=key)
            self.index(kk)
        return self.get(key)
    def scan(self, prefix=None, start=None, stop=None, limit=None, expand=False):
        keys = []
        if prefix:
            for obj in ArtifactIndex.objects.filter(key__startswith=prefix):
                key = obj.key.split("__")[-1]
                keys.append(key)
        elif start and stop:
            for obj in ArtifactIndex.objects.filter(key__gte=start, key__lte=stop):    
                key = obj.key.split("__")[-1]
                keys.append(key)
        return keys
    def index(self, key):
        obj, new = ArtifactIndex.objects.get_or_create(key=key)
        return obj.key
