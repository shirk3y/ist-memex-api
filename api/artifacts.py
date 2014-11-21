import sys
import time
import re
import zlib
import cbor
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
        docs = broker.search(index='timestamp',prefix='', limit=limit, expand=expand)
        response = []
        for doc in docs:
            if expand:
                response.append(broker.strip_indices(doc))
            else:  
                response.append(doc)
        return Response(response)
    def post(self, request, format=None):
        response = None
        broker = ArtifactBroker(settings.API_ARTIFACT_MANAGER_BACKEND)
        if type(request.DATA) is list:
            response = []
            for data in request.DATA: 
                response.append(broker.strip_indices(broker.save(data)))
        else:
            response = broker.strip_indices(broker.save(request.DATA))
        return Response(response)

class ArtifactItem(APIView):
    def get(self, request, key, format=None):
        broker = ArtifactBroker(settings.API_ARTIFACT_MANAGER_BACKEND)
        doc = broker.get(key)
        response = broker.strip_indices(doc)
        return Response(response)
    def put(self, request, key, format=None):
        broker = ArtifactBroker(settings.API_ARTIFACT_MANAGER_BACKEND)
        response = broker.strip_indices(broker.save(request.DATA, key))
        return Response(response)
    def delete(self, request, key, format=None):
        broker = ArtifactBroker(settings.API_ARTIFACT_MANAGER_BACKEND)
        response = broker.delete(key)
        return Response(response)

class ArtifactSearch(APIView):
    def get(self, request, index, value=None, prefix=None, start=None, end=None, format=None):
        limit = int(request.QUERY_PARAMS.get('limit', 1000))
        expand = bool(request.QUERY_PARAMS.get('expand', False))
        broker = ArtifactBroker(settings.API_ARTIFACT_MANAGER_BACKEND)
        docs = broker.search(index=index, value=value, prefix=prefix, start=start, end=end, limit=limit, expand=expand)
        response = []
        for doc in docs:
            if expand:
                response.append(broker.strip_indices(doc))
            else:
                response.append(doc)
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
                "properties": {
                    "method": {
                        "type": "string",
                        "pattern": "^[a-zA-Z]+$",
                    },
                }, 
                "required": [
                    "method",
                ],
            },
            "response": {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "number",
                        "minimum": 100,
                        "maximum": 999,
                        "multipleOf": 1.0,
                    },
                },
                "required": [
                    "status",
                ],
            },
            "indices": {
               "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "key": {
                            "type": "string",
                            "pattern": "^[0-9a-zA-Z$.!*()_+-]{1,48}$" ,
                        },
                        "value": {
                            "type": "string",
                            "pattern": "^[0-9a-zA-Z$.!*()_+-]{1,48}$" ,
                        },
                    },
                },
            },
        },
        "required": [
            "key",
            "url",
            "timestamp",
        ],
    }

    INDEX_SCHEMA = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "pattern": "^[0-9a-zA-Z$.!*()_+-]{1,48}$" ,
                },
                "value": {
                    "type": "string",
                    "pattern": "^[0-9a-zA-Z$.!*()_+-]{1,48}$" ,
                },
            },
            "required": [
                "key",
                "value",
            ],
        },
    }

    MAX_OBJECT_SIZE = 5 * 1024 * 1024 #5MB

    def serialize(self, doc):
        if self.backend.SERDE:
            return zlib.compress(cbor.dumps(doc))
        else:
            return doc

    def deserialize(self, data):
        if self.backend.SERDE:
            return cbor.loads(zlib.decompress(data))
        else:
            return data

    def get(self, key):
        data = self.backend.get(key)
        doc = self.deserialize(data)
        return doc

    def save(self, doc, key = None):
        doc, key = self.validate(doc, key)
        self.delete_indices(key)
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
        doc = self.validate_timestamp(doc)

        indices = []
        for idx in doc['indices']:
            try:
                jsonschema.validate(idx, ArtifactBroker.INDEX_SCHEMA)
                indices.append(idx)
            except:
                pass
        doc['indices'] = indices

        jsonschema.validate(doc, ArtifactBroker.SCHEMA)
        return doc, key

    def validate_key(self, doc, key = None):
        _key = doc.get('key', '').lower()
        # first make sure we have a key; if not, throw an error
        if not key:
            if _key:
                key = _key
            else:
                raise ValidationError("Required key not found".format(key1=key, key2=_key))
        key = key.lower()
        # now make sure the two match
        if key and _key and key != _key:
            raise ValidationError("Key mismatch; '{key1}' != '{key2}'".format(key1=key, key2=_key))
        doc['key'] = key
        return doc, key

    def validate_timestamp(self, doc):
        ts = doc.get('timestamp', int(time.time() * 1000))
        try:
            doc['timestamp'] = int(ts)
        except:
            raise ValidationError("Parser error: '{ts}' could not interpreted as a timestamp".format(ts=ts))
        self.add_index(doc, 'timestamp', self.flip_timestamp(doc['timestamp']))
        return doc

    def flip_timestamp(self, ts):
        flipped = []
        for cc in str(ts):
            flipped.append(str(9 - int(cc)))
        return "".join(flipped)

    def validate_index(self, key, value):
        pass
    
    def add_index(self, doc, key, value):
        try:
            self.validate_index(key, value)
            doc['indices'].append({'key':key, 'value':value})
        except ValidationError:
            pass

    def strip_indices(self, doc):
        internal_indices = ('timestamp',)
        indices = doc.get('indices')
        doc['indices'] = []
        for entry in indices:
            if not entry['key'] in internal_indices:
                doc['indices'].append(entry)
        return doc

class HbaseArtifactBackend(AbstractBackend):
    
    SERDE = True

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
    def delete(self, key):
        self.connection.table('artifact').delete(key)
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
    def delete_index(self, key):
        self.connection.table('artifact_index').delete(key)

class ModelArtifactBackend(AbstractBackend):

    SERDE = True 

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
    def delete(self, key):
        Artifact.objects.filter(key=key).delete()
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
    def delete_index(self, key):
        ArtifactIndex.objects.filter(key=key).delete()

class HbaseFlatArtifactBackend(AbstractBackend):

    SERDE = False

    def __init__(self):
        self.connection = happybase.Connection(host=settings.HBASE_HOST, port=int(settings.HBASE_PORT), table_prefix=settings.HBASE_TABLE_PREFIX)

    def get(self, key):
        row = self.connection.table('artifact').row(key)
        data = self.unpack(row, key)
        return data

    def put(self, key, data, indices=[]):
        row = self.pack(data)
        self.connection.table('artifact').put(key, row)
        for index in indices:
            kk = "{index_key}__{index_value}__{key}".format(index_key=index['key'], index_value=index['value'], key=key)
            self.index(kk)
        return self.get(key)

    def delete(self, key):
        self.connection.table('artifact').delete(key)

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

    def delete_index(self, key):
        self.connection.table('artifact_index').delete(key)

    def pack(self, data):
        request = data.get('request', {})
        response = data.get('response', {})
        indices = data.get('indices', [])
        row = {
            'f:url': data.get('url'),
            'f:timestamp': str(data.get('timestamp')),
            'f:request.method': request.get('method'),
            'f:request.client': zlib.compress(cbor.dumps(request.get('client', {}))),
            'f:request.headers': zlib.compress(cbor.dumps(request.get('headers', {}))),
            'f:request.body': zlib.compress(cbor.dumps(request.get('body', {}))),
            'f:response.status': str(response.get('status')),
            'f:response.server.hostname': response.get('server', {}).get('hostname'),
            'f:response.server.address': response.get('server', {}).get('address'),
            'f:response.headers': zlib.compress(cbor.dumps(response.get('headers', {}))),
            'f:response.body': zlib.compress(cbor.dumps(response.get('body', {}))),
        }
        keys = {}
        for ii in indices:
            kk = 'f:index.{}'.format(ii['key'])
            try:
                keys[kk] += 1
            except KeyError:
                keys[kk] = 0
            row["{}.{}".format(kk, str(keys[kk]))] = ii['value']
        return row

    def unpack(self, row, key):
        data = {
            'key': key,
            'url': row['f:url'],
            'timestamp': int(row['f:timestamp']),
            'request': {
                'method': row['f:request.method'],
                'client': cbor.loads(zlib.decompress(row['f:request.client'])),
                'headers': cbor.loads(zlib.decompress(row['f:request.headers'])),
                'body': cbor.loads(zlib.decompress(row['f:request.body'])),
            },
            'response': {
                'status': int(row['f:response.status']),
                'server': {
                    'hostname': row['f:response.server.hostname'],
                    'address': row['f:response.server.address'],
                },
                'headers': cbor.loads(zlib.decompress(row['f:response.headers'])),
                'body': cbor.loads(zlib.decompress(row['f:response.body'])),
            },
            'indices': [],
        }
        for kk, vv in row.items():
            mm = re.match(r"^f:index\.(?P<key>.*)\.[0-9]+$", kk)
            if mm is not None:
                data['indices'].append({'key':mm.group('key'), 'value': vv})
        return data
