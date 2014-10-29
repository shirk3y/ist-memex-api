from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpResponse

import json
import settings

def index(request):
    response = {"version":settings.MEMEX_API_VERSION}
    return HttpResponse(json.dumps(response), content_type="application/json")

