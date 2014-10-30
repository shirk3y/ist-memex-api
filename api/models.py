from django.db import models

class Log(models.Model):
    key = models.CharField(max_length=36,primary_key=True)
    data = models.BinaryField()

class Index(models.Model):
    key = models.CharField(max_length=255,primary_key=True)
