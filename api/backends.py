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

class MysqlBackend(AbstractBackend):
    def get(self, key):
        return { 'key': key }
