import time
import urllib2
import boto

from django.http import HttpResponse
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import APIException
from rest_framework.parsers import MultiPartParser

import settings

from brokers import GenericRecordBroker
from backends import AbstractBackend

class ImageItem(APIView):
    parser_classes = (MultiPartParser,)
    def get(self, request, key, format=None):
        broker = ImageBroker(settings.API_IMAGE_MANAGER_BACKEND)
        response = broker.get(key)
        return HttpResponse(response.read(), content_type=response.info().type)
    def put(self, request, key, format=None):
        broker = ImageBroker(settings.API_IMAGE_MANAGER_BACKEND)
        response = broker.save(request.FILES['data'], key)
        return HttpResponse(response.read(), content_type=response.info().type)
    def delete(self, request, key, format=None):
        broker = ImageBroker(settings.API_IMAGE_MANAGER_BACKEND)
        response = broker.delete(key)
        return Response(status=status.HTTP_204_NO_CONTENT)

class ImageBroker(GenericRecordBroker):
    def get(self, key):
        data = self.backend.get(key)
        return self.deserialize(data)

    def save(self, doc, key = None):
        self.backend.put(key, self.serialize(doc))
        return self.get(key)

    def delete(self, key):
        self.backend.delete(key)

    def serialize(self, doc):
        return doc

    def deserialize(self, data):
        return data

    def delete_indices(self, key):
        raise NotImplementedError

    def search(self, index, value=None, prefix=None, start=None, end=None, limit=None, expand=False):   
        raise NotImplementedError

class S3ImageBackend(AbstractBackend):
    def get(self, key):
        url = "https://s3.amazonaws.com/{}/{}".format(settings.S3_IMAGE_BUCKET, key.replace("_", "/"))
        return urllib2.urlopen(url)

    def put(self, key, data, indices=[]):
        retries = 1
        while True:
            try:
                s3conn = boto.connect_s3(settings.S3_ACCESS_KEY, settings.S3_SECRET_KEY)
                s3bucket = s3conn.get_bucket(settings.S3_IMAGE_BUCKET)
                s3key = boto.s3.key.Key(s3bucket)
                break
            except:
                if retries == 0:
                    raise APIException(detail="S3 Connection Error")
                retries -= 1
                time.sleep(2)

        path = key.replace("_", "/")    
        url = "https://s3.amazonaws.com/{}/{}".format(settings.S3_IMAGE_BUCKET, path)

        retries = 1
        while True:
            try:
                s3key.key = path
                s3key.set_contents_from_file(data)
                s3key.make_public()
                break
            except:
                if retries == 0:
                    raise APIException(detail="S3 Error")
                retries -= 1
                time.sleep(1)
        
    def delete(self, key):
        retries = 1
        while True:
            try:
                s3conn = boto.connect_s3(settings.S3_ACCESS_KEY, settings.S3_SECRET_KEY)
                s3bucket = s3conn.get_bucket(settings.S3_IMAGE_BUCKET)
                break
            except:
                if retries == 0:
                    raise APIException(detail="S3 Connection Error")
                retries -= 1
                time.sleep(2)

        path = key.replace("_", "/")    
 
        retries = 1
        while True:
            try:
                s3bucket.delete_key(path)
                break
            except:
                if retries == 0:
                    raise APIException(detail="S3 Error")
                retries -= 1
                time.sleep(1)
              
