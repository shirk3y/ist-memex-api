import sys
import time
import re
import uuid
import cbor
import zlib
import happybase
import jsonschema

from django.core.exceptions import ValidationError, ObjectDoesNotExist
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
        self.delete_indices(key)
        self.backend.put(key, self.serialize(doc), doc['indices'])
        return self.get(key)

    def delete(self, key):
        self.delete_indices(key)
        self.backend.delete(key)

    def validate(self, doc, key = None):
        raise NotImplementedError

    def serialize(self, doc):
        return zlib.compress(cbor.dumps(doc))

    def deserialize(self, data):
        return cbor.loads(zlib.decompress(data))

    def delete_indices(self, key):
        try:
            doc = self.get(key)
            for ii in doc['indices']:
                kk = "{}__{}__{}".format(ii['key'], ii['value'], key)
                self.backend.delete_index(kk)
        except:
            pass

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
                try:
                    _results.append(self.get(result))
                except ObjectDoesNotExist:
                    pass
            results = _results
        return results

    def validate_index(self, key, value):
        raise NotImplementedError

    def instantiate_backend(self, backend_class):
        module_name = '.'.join(backend_class.split('.')[:-1])
        __import__(module_name)
        module_obj = sys.modules[module_name]
        class_name = backend_class.split('.')[-1]
        class_obj = getattr(module_obj, class_name)
        return class_obj()

