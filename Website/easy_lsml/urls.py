from django.conf.urls import patterns, url
from easy_lsml import views

urlpatterns = patterns('',
        url(r'^$', views.index, name='index'),
        url(r'^xml/$', views.generate_xml, name='generate_xml'),
        url(r'^new/$', views.new_project, name='new_project'),
        url(r'^cheat/$', views.cheat, name='cheat')
    )

