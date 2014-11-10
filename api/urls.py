from django.conf.urls import patterns, include, url
from django.contrib import admin

from logs import LogList, LogItem, LogSearch

from artifacts import ArtifactList #, LogItem, LogSearch

urlpatterns = patterns('',
    url(r'^/?$', 'api.views.index'),
    url(r'^artifacts/?$', ArtifactList.as_view(), name='artifact-list'),
    url(r'^logs/?$', LogList.as_view(), name='log-list'),
    url(r'^logs/(?i)(?P<key>[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})/?$', 
            LogItem.as_view(), name='log-item'),
    url(r'^logs/where/(?i)(?P<index>[0-9a-zA-Z$.!*()_+-]{1,48})/is/(?P<value>[0-9a-zA-Z$.!*()_+-]{1,160})/?$', 
            LogSearch.as_view(), name='log-search-exact'),
    url(r'^logs/where/(?i)(?P<index>[0-9a-zA-Z$.!*()_+-]{1,48})/like/(?P<prefix>[0-9a-zA-Z$.!*()_+-]{1,160})/?$', 
            LogSearch.as_view(), name='log-search-prefix'),
    url(r'^logs/where/(?i)(?P<index>[0-9a-zA-Z$.!*()_+-]{1,48})/from/(?P<start>[0-9a-zA-Z$.!*()_+-]{1,160})/to/(?P<end>[0-9a-zA-Z$.!*()_+-]{1,160})/?$', 
            LogSearch.as_view(), name='log-search-range'),
    url(r'^debug/?$', 'api.views.debug'),
    url(r'^admin/', include(admin.site.urls)),
)
