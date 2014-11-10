import json

from django.http import HttpResponse
from rest_framework.response import Response
from rest_framework.decorators import api_view

import settings

from logs import LogBroker

@api_view(['GET'])
def index(request):
    response = {"version":settings.MEMEX_API_VERSION}
    return Response(response)

def debug(request):
    response = { }
    return HttpResponse(json.dumps(response, indent=2), content_type="application/json")
