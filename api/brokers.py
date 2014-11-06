import sys
import time
import re
import uuid
import cbor
import zlib
import happybase
import jsonschema

from django.core.exceptions import ValidationError
from django.utils.text import slugify

import settings

class GenericRecordBroker(object):
    def __init__(self, backend_class):
        self.backend = self.instantiate_backend(backend_class)

    def get(self, key):
        data = self.backend.get(key)
        return self.deserialize(data)

    def save(self, doc, key = None):
        doc, key = self.validate(doc, key)
        self.backend.put(key, self.serialize(doc), doc['indices'])
        return self.get(key)

    def validate(self, doc, key = None):
        raise NotImplementedError

    def serialize(self, doc):
        return zlib.compress(cbor.dumps(doc))

    def deserialize(self, data):
        return cbor.loads(zlib.decompress(data))

    def search(self, index, value=None, prefix=None, start=None, end=None, limit=None, expand=False):
        results = []
        if value is not None:
            self.validate_index(index, value)
            key = "{index}__{value}__".format(index=index, value=value)
            results = self.backend.scan(prefix=key, limit=limit, expand=expand)
        elif prefix is not None:
            self.validate_index(index, prefix)
            key = "{index}__{value}".format(index=index, value=prefix)
            results = self.backend.scan(prefix=key, limit=limit, expand=expand)
        elif start is not None and end is not None:
            self.validate_index(index, start)
            self.validate_index(index, end)
            start_key = "{index}__{value}".format(index=index, value=start)
            stop_key = "{index}__{value}".format(index=index, value=end)
            results = self.backend.scan(start=start_key, stop=stop_key, limit=limit, expand=expand)
        else:
            pass #TODO: bad request
        if expand:
            _results = []
            for result in results:
                _results.append(self.get(result))
            results = _results
        return results

    def validate_index(self, key, value):
        if not bool(re.compile('[0-9a-zA-Z$.!*()_+-]{1,48}').match(key)):
            raise ValidationError("Invalid index key '{key}'".format(key=key))
        if not bool(re.compile('[0-9a-zA-Z$.!*()_+-]{0,160}').match(value)):
            raise ValidationError("Invalid index value '{value}'".format(value=value))

    def instantiate_backend(self, backend_class):
        module_name = '.'.join(settings.API_LOG_MANAGER_BACKEND.split('.')[:-1])
        __import__(module_name)
        module_obj = sys.modules[module_name]
        class_name = settings.API_LOG_MANAGER_BACKEND.split('.')[-1]
        class_obj = getattr(module_obj, class_name)
        return class_obj()

class LogBroker(GenericRecordBroker):

    SCHEMA = { 
        "type": "object",
        "properties": {
            "key": { 
                "type": "string", 
                "pattern": "^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$" ,
            },
            "time": {
                "type": "object",
                "properties": {
                    "startedAt": {
                        "type": "number",
                        "minimum": 1000000000000,
                        "maximum": 9999999999999,
                        "multipleOf": 1.0,
                    },
                    "endedAt": {
                        "type": "number",
                        "minimum": 1000000000000,
                        "maximum": 9999999999999,
                        "multipleOf": 1.0,
                    },
                },
                "required": [
                    "startedAt",
                ],
                "additionalProperties": False,
            },
            "action": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": [
                            "SYSTEM",
                            "USER"
                        ],
                    },
                    "description": {
                        "type": "string",
                        "minLength": 1,
                    },
                    "workflow": {
                        "type": "string",
                        "minLength": 1,
                    },
                    "activity": {
                        "type": "string",
                        "minLength": 1,
                    },
                    "inferred": {
                        "type": "boolean",
                    },
                },
                "required": [
                    "type", 
                    "description",
                    "inferred",
                ],
                "additionalProperties": False,
            },
            "client": {
                "type": "object",
                "properties": {
                    "ipAddress": {
                        "type": "string",
                        "minLength": 1,
                    }, 
                    "userId": { 
                        "type": "string",
                        "minLength": 1,
                    }, 
                    "sessionId": { 
                        "type": "string",
                        "minLength": 1,
                    }, 
                    "userAgent": { 
                        "type": "string",
                        "minLength": 1,
                    },
                },
                "additionalProperties": False,
            },
            "component": {
                "type": "object",   
                "properties": {
                    "apiLanguage": { 
                        "type": "string",
                        "minLength": 1,
                    }, 
                    "apiVersion": { 
                        "type": "string",
                        "minLength": 1,
                    }, 
                    "name": { 
                        "type": "string",
                        "minLength": 1,
                    }, 
                    "version": { 
                        "type": "string",
                        "minLength": 1,
                    },
                },
                "required": [
                    "name",
                    "version",
                    "apiVersion",
                ],
                "additionalProperties": False,
            },
            "acl": {
                "type": "object",
                "properties": {
                    "privacy": {
                        "type": "string",
                        "enum": [
                            "public",
                            "private",
                            "controlled",
                        ],
                    },
                    "controls": { },
                },
                "required": [
                    "privacy",
                ],
                "additionalProperties": False,
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
                    }
                }
                
            },
            "details": { } 
        },
        "required": [
            "key", 
            "time", 
            "action",
            "component",
        ],
        "additionalProperties": False,
    }

    def get(self, key):
        data = self.backend.get(key)
        doc = self.deserialize(data)
        doc = self.strip_indices(doc)
        return doc

    def search(self, index, value=None, prefix=None, start=None, end=None, limit=None, expand=False):
        if index in ('time.startedAt', 'time.endedAt'):
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
        doc = self.validate_time(doc)
        doc = self.validate_action(doc)

        doc = self.validate_component(doc)
        doc = self.validate_acl(doc)

        jsonschema.validate(doc, LogBroker.SCHEMA)
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

    def validate_time(self, doc):
        valid_children = ('startedAt', 'endedAt')
        _section = doc.get('time', {})
        for key in _section:
            if not key in valid_children:
                raise ValidationError("Schema error: 'time.{key}' is not allowed".format(key=key))
        _start = _section.get('startedAt')
        _end = _section.get('endedAt')
        if not _start:
            if not _end:
                _start = int(time.time() * 1000)
            else:
                raise ValidationError("Schema error: 'time.startedAt' is missing")
        if _start:
            _section['startedAt'] = self.validate_timestamp(_start)
            self.add_index(doc, 'time.startedAt', self.flip_timestamp(_start))
        if _end:
            _section['endedAt'] = self.validate_timestamp(_end)
            self.add_index(doc, 'time.endedAt', self.flip_timestamp(_end))
        doc['time'] = _section
        return doc
    
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
    
    def validate_action(self, doc):
        _section = doc.get('action', {})
        _section['type'] = _section.get('type', '').upper()
        doc['action'] = _section
        return doc

    def validate_client(self, doc):
        valid_children = ('ipAddress', 'userId', 'sessionId', 'userAgent')
        _section = doc.get('client', {})
        for key in _section:
            if not key in valid_children:
                raise ValidationError("Schema error: 'client.{key}' is not allowed".format(key=key))
        if 'sessionId' in _section and _section['sessionId']:
            self.add_index(doc, 'client.sessionId', _section['sessionId']) 
        if 'userId' in _section and _section['userId']:
            self.add_index(doc, 'client.userId', _section['userId']) 
        if 'ipAddress' in _section and _section['ipAddress']:
            self.add_index(doc, 'client.ipAddress', _section['ipAddress']) 
        return doc

    def validate_component(self, doc):
        _section = doc.get('component', {})
        _section['apiVersion'] = _section.get("apiVersion", settings.MEMEX_API_VERSION)
        doc['component'] = _section
        self.add_index(doc, 'component.name', slugify(unicode('{name}_{version}'.format(name=_section['name'], version=_section['version']))))
        return doc

    def validate_acl(self, doc):
        _section = doc.get('acl', {})
        _privacy = _section.get('privacy', 'public').lower()
        if _privacy == 'controlled':
            _controls = _section.get('controls')
            if not _controls:
                raise ValidationError("Schema error: 'acl.controls' is missing") 
        _section['privacy'] = _privacy
        return doc

    def add_index(self, doc, key, value):
        try:
            self.validate_index(key, value)
            doc['indices'].append({'key':key, 'value':value})
        except ValidationError:
            pass

    def strip_indices(self, doc):
        internal_indices = ('time.startedAt', 'time.endedAt', 'action.workflow', 'action.activity', 'client.sessionId', 'client.userId', 'client.ipAddress', 'component.name')
        indices = doc.get('indices')
        doc['indices'] = []
        for entry in indices:
            if not entry['key'] in internal_indices:
                doc['indices'].append(entry)
        return doc
