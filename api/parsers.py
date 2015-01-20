import cbor
from rest_framework.parsers import BaseParser

class CBORParser(BaseParser):

    media_type = 'application/cbor'

    def parse(self, stream, media_type=None, parser_context=None):
        return cbor.loads(stream.read())
