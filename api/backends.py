class AbstractBackend:
    def get(self, key):
        raise NotImplementedError
    def put(self, key, data, indices=[]):
        raise NotImplementedError
    def delete(self, key):
        raise NotImplementedError
    def scan(self, prefix=None, start=None, stop=None, limit=None, expand=False):
        raise NotImplementedError
    def index(self, key):
        raise NotImplementedError
    def delete_index(self, key):
        raise NotImplementedError
