import sys
import time
import re
import uuid
import cbor
import zlib
import happybase

from django.core.exceptions import ValidationError

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
        doc, key = self.validate_key(doc, key)
        doc = self.validate__tree(doc)
        doc = self.validate_time(doc)
        doc = self.validate_action(doc)
        doc = self.validate_client(doc)
        doc = self.validate_component(doc)
        doc = self.validate_acl(doc)
        doc = self.validate_details(doc)
        doc = self.validate_indices(doc)
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

    def validate__tree(self, doc):
        allowed_children = ('key', 'time', 'action', 'client', 'component', 'acl', 'details', 'indices')
        required_children = ('action', 'component', 'acl')
        for key in doc:
            if not key in allowed_children:
                raise ValidationError("Schema error: '{key}' is not allowed".format(key=key))
        for key in required_children:
            _section = doc.get(key)
            if _section is None:
                raise ValidationError("Schema error: '{key}' is required".format(key=key))
            if type(_section) != type(dict()):
                raise ValidationError("Schema error: '{key}' may not be type '{_type}'".format(key=key, _type=type(_section).__name__))
        doc['indices'] = doc.get('indices', [])
        return doc

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
        valid_children = ('type', 'description', 'workflow', 'activity')
        _section= doc.get('action', {})
        for key in _section:
            if not key in valid_children:
                raise ValidationError("Schema error: 'action.{key}' is not allowed".format(key=key))

        _type = _section.get('type', '').upper()
        if not _type:
            raise ValidationError("Schema error: 'action.type' is required")
        if not _type in ("SYSTEM", "USER"):
            raise ValidationError("Schema error: invalid value for 'action.type'")
        _section["type"] = _type

        _desc = _section.get('description', '')
        if not _desc:
            raise ValidationError("Schema error: 'action.description' is required")

        if 'workflow' in _section and _section['workflow']:
            self.add_index(doc, 'action.workflow', _section['workflow'])
        if 'activity' in _section and _section['activity']:
            self.add_index(doc, 'action.activity', _section['activity'])

        doc["action"] = _section
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
        valid_children = ('apiLanguage', 'apiVersion', 'name', 'version')
        required_children = ('apiVersion', 'name', 'version')
        _section = doc.get('component', {})
        for key in _section:
            if not key in valid_children:
                raise ValidationError("Schema error: 'component.{key}' is not allowed".format(key=key))
        for key in required_children:
            if not key in _section or not _section[key]:
                raise ValidationError("Schema error: 'component.{key}' is required".format(key=key))
        self.add_index(doc, 'component.name', '{name}_{version}'.format(name=_section['name'], version=_section['version']).strip())
        return doc

    def validate_acl(self, doc):
        valid_children = ('privacy', 'controls')
        _section = doc.get('acl', {})
        for key in _section:
            if not key in valid_children:
                raise ValidationError("Schema error: 'acl.{key}' is not allowed".format(key=key))
        _privacy = _section.get('privacy', '').lower()
        if not _privacy:
            _privacy = 'public'
        if not _privacy in ('public', 'private', 'controlled'):
            raise ValidationError("Schema error: invalid value for 'acl.privacy'")
        if _privacy == 'controlled':
            _controls = _section.get('controls')
            if not _controls:
                raise ValidationError("Schema error: 'acl.controls' is missing") 
        _section['privacy'] = _privacy
        return doc

    def validate_details(self, doc):
        return doc

    def validate_indices(self, doc):
        _section = doc.get('indices', [])
        for entry in _section:
            key = entry.get('key')
            value = entry.get('value')
            self.validate_index(key, value)
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
