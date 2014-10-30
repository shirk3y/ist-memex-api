from django.contrib import admin
from models import *

class LogAdmin(admin.ModelAdmin):
    list_display = ('key',)

class IndexAdmin(admin.ModelAdmin):
    list_display = ('key',)

admin.site.register(Log, LogAdmin)
admin.site.register(Index, IndexAdmin)

