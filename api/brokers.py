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
        module_name = '.'.join(backend_class.split('.')[:-1])
        __import__(module_name)
        module_obj = sys.modules[module_name]
        class_name = backend_class.split('.')[-1]
        class_obj = getattr(module_obj, class_name)
        return class_obj()

