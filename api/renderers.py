import cbor
from rest_framework.renderers import BaseRenderer

class CBORRenderer(BaseRenderer):
    media_type = 'application/cbor'
    format = 'cbor'

    def render(self, data, media_type=None, renderer_context=None):
        return cbor.dumps(data)
