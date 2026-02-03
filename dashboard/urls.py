from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="dashboard"),
    path("api/events/", views.events_api, name="events_api"),
    path("api/conversations/", views.conversations_api, name="conversations_api"),
    path("api/messages/", views.messages_api, name="messages_api"),
    path("api/dashboard/", views.dashboard_api, name="dashboard_api"),
]
