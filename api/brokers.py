import sys
import cbor
import zlib
import happybase

import settings

class GenericRecordBroker():
    def __init__(self, backend_class):
        self.backend = self.instantiate_backend(backend_class)

    def get(self, key):
        data = self.backend.get(key)
        return self.deserialize(data)

    def save(self, key, doc):
        self.validate(doc)
        return self.deserialize(self.backend.put(key, self.serialize(doc)))

    def validate(self, doc):
        raise NotImplementedError

    def serialize(self, doc):
        return zlib.compress(cbor.dumps(doc))

    def deserialize(self, data):
        return cbor.loads(zlib.decompress(data))

    def search(self, index, value=None, prefix=None, start=None, end=None, limit=None):
        if value is not None:
            self.validate_index(index, value)
            key = "{index}__{value}__".format(index=index, value=value)
            return self.backend.scan(prefix=key, limit=limit)
        elif prefix is not None:
            self.validate_index(index, prefix)
            key = "{index}__{value}".format(index=index, value=prefix)
            return self.backend.scan(prefix=key, limit=limit)
        elif start is not None and end is not None:
            self.validate_index(index, start)
            self.validate_index(index, end)
            start_key = "{index}__{value}".format(index=index, value=start)
            end_key = "{index}__{value}".format(index=index, value=end)
            return self.backend.scan(start=start_key, stop=stop_key, limit=limit)
        else:
            pass #TODO: bad request

    def instantiate_backend(self, backend_class):
        module_name = '.'.join(settings.API_LOG_MANAGER_BACKEND.split('.')[:-1])
        __import__(module_name)
        module_obj = sys.modules[module_name]
        class_name = settings.API_LOG_MANAGER_BACKEND.split('.')[-1]
        class_obj = getattr(module_obj, class_name)
        return class_obj()

class LogBroker(GenericRecordBroker):
    def validate(self, doc):
        return True
