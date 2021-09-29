from y_day.forms import InputDateForm
import pandas as pd
import numpy as np
from django.shortcuts import redirect, render
from django.http import HttpResponse, request
from django.conf import settings
from numpy.lib.function_base import diff
from pandas.core.indexes import timedeltas
from pandas.core.tools.datetimes import to_datetime
from pandas.core.tools.timedeltas import to_timedelta
import pyodbc
from matplotlib import pyplot as plt
import datetime
from django.shortcuts import render, get_object_or_404
from django.http import HttpResponseRedirect
from django.urls import reverse



#Setting OPTIMA server info
optima = settings.OPTIMA

json_server = optima['server']
json_database = optima['database']
json_username = optima['username']
json_password = optima['password']
json_port = optima['port']
json_driver = optima['driver']

# Create your views here.
def sec_to_hours(seconds):
    a=str(seconds//3600)
    b=str((seconds%3600)//60)
    d="{},{}".format(a, b)
    return d


def index(request):


    return HttpResponse('Hi')

def y_day_workers(request, date='', template_name = 'y_day/y_day_workers.html'):
    cnn = pyodbc.connect('DRIVER='+json_driver+';SERVER='+json_server+';PORT='+json_port+';DATABASE='+json_database+';UID='+json_username+
                ';PWD='+json_password, timeout=300)
    df = pd.DataFrame()
    i = 0       #Set how many days to substract from current day - later on can edit it to a input datatype
    if date=='':
        sub = datetime.date.today()
    else:
        sub = date
    while df.empty:
        sql_q = ("""
        SELECT  
        CDN_HANSEN.dbo.zProdPracownicy.Nazwisko,
        CDN_HANSEN.dbo.zProdPracownicy.Imie,
        CDN_HANSEN.dbo.zProdRcpZdarzenia.PracownikId AS id,
        DataCzas,
        CDN_HANSEN.dbo.zProdRcpZdarzenia.Typ AS "Opis operacji",
        CDN_HANSEN.dbo.zProdPracownicy.Opis AS Stanowisko,
        CDN_HANSEN.dbo.zProdZlecenie.PZN_Cecha2 AS Zlecenie,
        CDN_HANSEN.dbo.zProdZlecenieSElem.PZS_TwrNazwa AS Czynność
        FROM CDN_HANSEN.dbo.zProdRcpZdarzenia
        FULL JOIN CDN_HANSEN.dbo.zProdPracownicy ON CDN_HANSEN.dbo.zProdRcpZdarzenia.PracownikId = CDN_HANSEN.dbo.zProdPracownicy.Id
        FULL JOIN CDN_HANSEN.dbo.zProdZlecenieSElem ON CDN_HANSEN.dbo.zProdRcpZdarzenia.OperacjaID = CDN_HANSEN.dbo.zProdZlecenieSElem.PZS_Id
        FULL JOIN CDN_HANSEN.dbo.zProdZlecenieElem ON CDN_HANSEN.dbo.zProdZlecenieSElem.PZS_PZENumer = CDN_HANSEN.dbo.zProdZlecenieElem.PZE_Id
        FULL JOIN CDN_HANSEN.dbo.zProdZlecenie ON CDN_HANSEN.dbo.zProdZlecenieElem.PZE_PZNNumer = CDN_HANSEN.dbo.zProdZlecenie.PZN_Id
        WHERE (SELECT CONVERT(VARCHAR(10), DataCzas, 23)) = '"""+str(sub)+"""'
        AND CDN_HANSEN.dbo.zProdPracownicy.Opis != 'x'
        AND CDN_HANSEN.dbo.zProdRcpZdarzenia.Opis LIKE ('%operacji')
        ORDER BY CDN_HANSEN.dbo.zProdPracownicy.Nazwisko, CDN_HANSEN.dbo.zProdPracownicy.Imie, DataCzas,CDN_HANSEN.dbo.zProdPracownicy.Opis
        """)
        i = i + 1 #iterate in case if day has no data in it
        df = pd.read_sql_query(sql_q,cnn) 
    df['Czas na zleceniu'] = 0
    df['Status'] = 0
    for index in df.index:
        if df['Opis operacji'][index] == 1:
                difference_set = (pd.Timestamp.now() - df['DataCzas'][index])
                difference_set = difference_set.to_pytimedelta()
                difference_set = difference_set.total_seconds()
                df['Czas na zleceniu'][index] = difference_set
                df['Status'][index] = "still working" 
        if index > 0:
            if df['id'][index] == df['id'][index-1]:
                if df['Zlecenie'][index] == df['Zlecenie'][index-1]:
                    if df['Opis operacji'][index] == 2 and df['Opis operacji'][index-1] == 1:
                        difference_set = (df['DataCzas'][index] - df['DataCzas'][index - 1])
                        difference_set = difference_set.to_pytimedelta()
                        difference_set = difference_set.total_seconds()
                        df['Czas na zleceniu'][index] = difference_set
                        df['Status'][index] = "Done"
                        df = df.drop(index = index-1)
                """elif df['Opis operacji'][index] == 2 and df['Opis operacji'][index-1] == 2:
                        df['Status'][index] = "ERROR" """ 
                #Uncomment above to show "ERROR" in status when mistake on RCP
    df = df.drop(columns=['Opis operacji', 'DataCzas'])
    df = df.sort_values(by = ['Zlecenie', 'Nazwisko', 'Imie', 'Status', 'id'])
    df = df.reset_index()
    JobSummaryTime = df.groupby(['Zlecenie']).sum(['Czas na zleceniu']).reset_index()
    JobSummaryTime = JobSummaryTime.drop(columns=['id', 'index'])
    JobSummaryTime = JobSummaryTime.rename(columns={"Czas na zleceniu" : "SummaryTime"})
    df = pd.merge(df, JobSummaryTime, how='outer', left_on='Zlecenie', right_on='Zlecenie')
    PersonSummaryTime = df.groupby(['Zlecenie','id','Nazwisko', 'Imie']).sum(['Czas na zleceniu'])
    PersonSummaryTime = PersonSummaryTime.reset_index()
    PersonSummaryTime = PersonSummaryTime.drop(columns=['index', 'SummaryTime', 'Nazwisko', 'Imie'])
    PersonSummaryTime = PersonSummaryTime.rename(columns={'Czas na zleceniu': 'PersonSumTime'})
    df = pd.merge(df, PersonSummaryTime, how='outer', left_on=['Zlecenie', 'id'], right_on=['Zlecenie', 'id'])
    df = df.drop(columns=['Czas na zleceniu', 'index'])
    df.reset_index()
    df['PersonSumTime'] = round((df['PersonSumTime'] / 3600), 2)
    df['SummaryTime'] = round((df['SummaryTime'] / 3600), 2)
    df = df.drop_duplicates()
    ff = pd.pivot(df, index=['Zlecenie', 'SummaryTime','Stanowisko', 'Nazwisko', 'Imie', 'id'], columns=[]) 
    ff = ff.sort_values(by = ['Zlecenie', 'Stanowisko', 'Nazwisko', 'Imie'])
    ff.reset_index()
    final = ff.to_html(justify="center", classes="table table-hover")
    context = {
        'final' : final,
    }
    return render(request, template_name, context)
    

def y_day_workers_date(request, template_name = 'y_day/date_form.html', ):
    if request.method == 'GET':
        today = datetime.date.today()
        form = InputDateForm(None, initial={'input_data': today})

    if request.method == 'POST':
        form = InputDateForm(request.POST)
        if form.is_valid():
            data = form.cleaned_data['input_data']
            data = data.strftime('%Y-%m-%d')
            return redirect('y_day_workers', date = data)

    context = { 
        'form' : form,
    }
    return render(request, template_name, context)
    