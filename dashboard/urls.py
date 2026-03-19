from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="dashboard"),
    path("api/events/", views.events_api, name="events_api"),
    path("api/conversations/", views.conversations_api, name="conversations_api"),
    path("api/messages/", views.messages_api, name="messages_api"),
    path("api/dashboard/", views.dashboard_api, name="dashboard_api"),
    path(
        "api/dashboard/stages/",
        views.dashboard_stage_stratification_api,
        name="dashboard_stage_stratification_api",
    ),
    path(
        "api/dashboard/dead/",
        views.dead_conversations_api,
        name="dead_conversations_api",
    ),
    path(
        "api/dashboard/alerts/",
        views.alerts_api,
        name="alerts_api",
    ),
    path(
        "webhook/smclick/",
        views.smclick_webhook,
        name="smclick_webhook",
    ),
    path("api/debug/smclick/", views.smclick_debug, name="smclick_debug"),
]
