from django.conf.urls import patterns, include, url
from django.contrib import admin

urlpatterns = patterns('',
    url(r'^/?$', 'api.views.index'),
    url(r'^debug/?$', 'api.views.debug'),
    url(r'^admin/', include(admin.site.urls)),
)
