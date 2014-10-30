from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpResponse
from rest_framework.response import Response
from rest_framework.decorators import api_view

import json
import settings

@api_view(['GET'])
def index(request):
    response = {"version":settings.MEMEX_API_VERSION}
    return Response(response)

def debug(request):
    response = {
        'auth': request.META['HTTP_AUTHORIZATION']
    }
    return HttpResponse(json.dumps(response, indent=2), content_type="application/json")
