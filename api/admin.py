from django.contrib import admin
from models import *

class GenericModelAdmin(admin.ModelAdmin):
    list_display = ('key',)

admin.site.register(Log, GenericModelAdmin)
admin.site.register(Index, GenericModelAdmin)

admin.site.register(Artifact, GenericModelAdmin)
admin.site.register(ArtifactIndex, GenericModelAdmin)
