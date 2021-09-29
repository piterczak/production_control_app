from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('y_day_workers', views.y_day_workers, name='y_day_workers'),
    path('y_day_workers/<str:date>/', views.y_day_workers, name='y_day_workers'),
    path('y_day_workers/<str:year>/<str:week>/', views.y_day_workers, name='y_day_workers'),
    path('y_day_workers_date/', views.y_day_workers_date, name = 'y_day_workers_date'),
    path('order_details/<str:order>', views.order_details, name = 'order_details'),
    path('order_details/<str:order>/str:<order_2>', views.order_details, name = 'order_details'),
    path('order_view/', views.order_view, name = 'order_view'),
    #path('generate_excel/', views.generate_excel, name = 'generate_excel'),

    ]
