from django.core.exceptions import ValidationError
from y_day.forms import InputDateForm, InputOrderForm, InputWeekForm
import pandas as pd
from django.shortcuts import redirect, render
from django.http import HttpResponse
from django.conf import settings
import pyodbc
import datetime
from django.shortcuts import render
import os
from win32com.client import Dispatch


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

def y_day_workers(request, date='', week='', year='', template_name = 'y_day/y_day_workers.html'):  #Widok odpowiedzialny za stworzenie tabeli wyświetlającej pracowników na zleceniach w danym dniu/tygodniu
    cnn = pyodbc.connect('DRIVER='+json_driver+';SERVER='+json_server+';PORT='+json_port+';DATABASE='+json_database+';UID='+json_username+
                ';PWD='+json_password, timeout=300)
    df = pd.DataFrame()
    if week != '':
        query_piece = """
        WHERE (DATEPART(week, DataCzas)) = ' """ + week + """ '
        AND (DATEPART(year, DataCzas)) = ' """ + year + """ '
        AND CDN_HANSEN.dbo.zProdRcpZdarzenia.Opis LIKE ('%operacji')
        ORDER BY CDN_HANSEN.dbo.zProdPracownicy.Nazwisko, CDN_HANSEN.dbo.zProdPracownicy.Imie, DataCzas,CDN_HANSEN.dbo.zProdPracownicy.Opis
        """
    else:
        query_piece = """WHERE (SELECT CONVERT(VARCHAR(10), DataCzas, 23)) = '"""+str(date)+"""'
        AND CDN_HANSEN.dbo.zProdRcpZdarzenia.Opis LIKE ('%operacji')
        ORDER BY CDN_HANSEN.dbo.zProdPracownicy.Nazwisko, CDN_HANSEN.dbo.zProdPracownicy.Imie, DataCzas,CDN_HANSEN.dbo.zProdPracownicy.Opis
        """
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
        """ + str(query_piece) + """""")
        df = pd.read_sql_query(sql_q,cnn) 
    df['Czas na zleceniu'] = 0
    for index in df.index:
        if df['Opis operacji'][index] == 1:
            difference_set = (pd.Timestamp.now() - df['DataCzas'][index])
            difference_set = difference_set.to_pytimedelta()
            difference_set = difference_set.total_seconds()
            df['Czas na zleceniu'][index] = difference_set
        if index > 0:
            if df['id'][index] == df['id'][index-1]:
                if df['Zlecenie'][index] == df['Zlecenie'][index-1]:
                    if df['Opis operacji'][index] == 2 and df['Opis operacji'][index-1] == 1:
                        difference_set = (
                            df['DataCzas'][index] - df['DataCzas'][index - 1])
                        difference_set = difference_set.to_pytimedelta()
                        difference_set = difference_set.total_seconds()
                        df['Czas na zleceniu'][index] = difference_set
                        df = df.drop(index=index-1)
                """elif df['Opis operacji'][index] == 2 and df['Opis operacji'][index-1] == 2:
                        df['Status'][index] = "ERROR" """
                #Uncomment above to show "ERROR" in status when mistake on RCP
    df = df.drop(columns=['Opis operacji', 'DataCzas'])
    df = df.sort_values(by=['Zlecenie', 'Nazwisko', 'Imie', 'id'])
    df = df.reset_index()
    JobSummaryTime = df.groupby(['Zlecenie']).sum(['Czas na zleceniu']).reset_index()
    JobSummaryTime = JobSummaryTime.drop(columns=['id', 'index'])
    JobSummaryTime = JobSummaryTime.rename(columns={"Czas na zleceniu" : "Łączny czas zlecenia"})
    df = pd.merge(df, JobSummaryTime, how='outer', left_on='Zlecenie', right_on='Zlecenie')
    PersonSummaryTime = df.groupby(['Zlecenie','id','Nazwisko', 'Imie', 'Czynność']).sum(['Czas na zleceniu'])
    PersonSummaryTime = PersonSummaryTime.reset_index()
    PersonSummaryTime = PersonSummaryTime.drop(columns=['index', 'Łączny czas zlecenia', 'Nazwisko', 'Imie'])
    PersonSummaryTime = PersonSummaryTime.rename(columns={'Czas na zleceniu': 'Czas osoby na danej czynności w zleceniu'})
    df = pd.merge(df, PersonSummaryTime, how='outer', left_on=['Zlecenie','Czynność', 'id'], right_on=['Zlecenie', 'Czynność' ,'id'])
    df = df.drop(columns=['Czas na zleceniu', 'index'])
    df.reset_index()
    df['Czas osoby na danej czynności w zleceniu'] = round((df['Czas osoby na danej czynności w zleceniu'] / 3600), 2)
    df['Łączny czas zlecenia'] = round((df['Łączny czas zlecenia'] / 3600), 2)
    df = df.drop_duplicates()
    df = df.drop(columns='Stanowisko')
    ff = pd.pivot(df, index=['Zlecenie', 'Łączny czas zlecenia', 'Nazwisko', 'Imie', 'id'], columns=[]) 
    ff = ff.sort_values(by = ['Zlecenie', 'Nazwisko', 'Imie'])
    ff.reset_index()
    final = ff.to_html(justify="center", classes="data-2 table table-hover")

    if request.method == 'GET':
        message = "Click to export report to excel (your download folder)"
        context = {
        'final' : final,
        'message' : message,
        }
        return render(request, template_name, context)
    if request.method == 'POST':
        if date != '':
            file_name = date
        else:
            file_name = ''+year+'- week nr. '+week
        download_folder = os.path.expanduser("~")+"/Downloads/"
        passed_df = ff.to_excel(excel_writer = download_folder + file_name + ' - excel_report.xlsx', float_format = '%2f', index=True, encoding = 'utf-8', header=True)
        passed_df
        excel = Dispatch('Excel.Application')       
        wb = excel.Workbooks.Open(download_folder + file_name + ' - excel_report.xlsx')
        excel.Worksheets(1).Activate()
        excel.ActiveSheet.Columns.AutoFit()
        excel.ActiveWindow.Zoom = 85
        wb.Save()
        wb.Close()
        message = "Excel generated at your local 'Downloads' folder"
        context = {
        'final' : final,
        'message' : message,
        }
        return render(request, template_name, context)

def y_day_workers_date(request, template_name = 'y_day/date_form.html' ):       #Widok odpowiedzialny za wyświetlenie formularza w którym wprowadzana będzie data lub tydzień
    today = datetime.date.today()
    today_week = today.strftime('%W')
    today_year = today.strftime("%Y")
    form = InputDateForm(None, initial={'input_data': today})
    form2 = InputWeekForm(None, initial={'input_week': today})
    context = { 
        'form' : form,
        'form2' : form2,
    }
    if request.method == 'GET':
        return render(request, template_name, context)
            
    if request.method == 'POST':
        if 'input_data' in request.POST:
            form = InputDateForm(request.POST)
            if form.is_valid():
                data = form.cleaned_data['input_data']
                data = data.strftime('%Y-%m-%d')
                return redirect('y_day_workers', date=data)
            else:
                context = {
                    'form': form,
                    'form2': form2,
                }
                return render(request, template_name, context)
        elif 'input_week' in request.POST:
            form2 = InputWeekForm(request.POST)
            data2 = form2['input_week']
            week = data2.value()[6:]
            year = data2.value()[:4]
            if week <= today_week and year <= today_year or year < today_year:
                return redirect('y_day_workers', year=year, week=week)
            else:
                context = {
                    'form': form,
                    'form2': form2,
                }
                raise ValidationError('WRONG DATE, RETURN TO PREVIOUS SITE')
    
def order_details(request, order='', template_name = 'y_day/order_details.html'):   #Widok odpowiedzialny za wyświetlenie tabeli zawierającej informację o zleceniu od pierwszych wpisów w bazie danych
    cnn = pyodbc.connect('DRIVER='+json_driver+';SERVER='+json_server+';PORT='+json_port+';DATABASE='+json_database+';UID='+json_username +
                         ';PWD='+json_password, timeout=300)
    df = pd.DataFrame()
    licznik = 0
    query_piece = order
    query_piece = str(query_piece)
    if "#2F" in query_piece:
        query_piece = query_piece.replace("#2F", "/")
    if order != '':
        while df.empty:
            sql_q = ("""
            SELECT  
            CDN_HANSEN.dbo.zProdPracownicy.Nazwisko,
            CDN_HANSEN.dbo.zProdPracownicy.Imie,
            CDN_HANSEN.dbo.zProdRcpZdarzenia.PracownikId AS id,
            DataCzas,
            CDN_HANSEN.dbo.zProdRcpZdarzenia.Typ AS "Opis operacji",
            CDN_HANSEN.dbo.zProdPracownicy.Opis AS 'Stanowisko',
            CDN_HANSEN.dbo.zProdZlecenie.PZN_Cecha2 AS 'Zlecenie',
            CDN_HANSEN.dbo.zProdZlecenieSElem.PZS_TwrNazwa AS 'Czynność'
            FROM CDN_HANSEN.dbo.zProdRcpZdarzenia
            FULL JOIN CDN_HANSEN.dbo.zProdPracownicy ON CDN_HANSEN.dbo.zProdRcpZdarzenia.PracownikId = CDN_HANSEN.dbo.zProdPracownicy.Id
            FULL JOIN CDN_HANSEN.dbo.zProdZlecenieSElem ON CDN_HANSEN.dbo.zProdRcpZdarzenia.OperacjaID = CDN_HANSEN.dbo.zProdZlecenieSElem.PZS_Id
            FULL JOIN CDN_HANSEN.dbo.zProdZlecenieElem ON CDN_HANSEN.dbo.zProdZlecenieSElem.PZS_PZENumer = CDN_HANSEN.dbo.zProdZlecenieElem.PZE_Id
            FULL JOIN CDN_HANSEN.dbo.zProdZlecenie ON CDN_HANSEN.dbo.zProdZlecenieElem.PZE_PZNNumer = CDN_HANSEN.dbo.zProdZlecenie.PZN_Id
            WHERE CDN_HANSEN.dbo.zProdZlecenie.PZN_Cecha2 LIKE ('""" + str(query_piece) + """%')
            AND CDN_HANSEN.dbo.zProdRcpZdarzenia.Opis LIKE ('%operacji')
            ORDER BY CDN_HANSEN.dbo.zProdPracownicy.Nazwisko, CDN_HANSEN.dbo.zProdPracownicy.Imie, DataCzas,CDN_HANSEN.dbo.zProdPracownicy.Opis """)
            df = pd.read_sql_query(sql_q, cnn)
            licznik = licznik + 1
            if licznik > 2:
                warning = "Brak wpisów na temat danego zlecenia w bazie!"
                context ={
                    'warning' : warning
                }
                return render(request, template_name, context)
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
                            difference_set = (
                                df['DataCzas'][index] - df['DataCzas'][index - 1])
                            difference_set = difference_set.to_pytimedelta()
                            difference_set = difference_set.total_seconds()
                            df['Czas na zleceniu'][index] = difference_set
                            df['Status'][index] = "Done"
                            df = df.drop(index=index-1)   
        df ['DataCzas'] = pd.to_datetime(df['DataCzas']).dt.date
        df = df.drop(columns=['Opis operacji'])
        df = df.sort_values(by=['DataCzas'])
        df.reset_index()
        df = df.drop_duplicates()
        df['Czas na zleceniu'] = round((df['Czas na zleceniu'] / 3600), 2)
        sorted_df = df.groupby(['Zlecenie', 'Nazwisko', 'Imie', 'DataCzas','id']).sum(['']).reset_index()
        suma_dnia = sorted_df.groupby(['DataCzas', 'Zlecenie']).sum().reset_index()
        if 'id' in suma_dnia:
            suma_dnia = suma_dnia.drop(columns='id')
        suma_zlecenia = suma_dnia.groupby(['Zlecenie']).sum().reset_index()
        suma_zlecenia = suma_zlecenia.rename(columns = {"Czas na zleceniu" : "Suma zlecenia"})
        suma_dnia = suma_dnia.groupby(['DataCzas']).sum().reset_index()
        count_worker_summary_time = sorted_df.groupby(['Zlecenie','id', 'Nazwisko', 'Imie']).sum('Czas na zleceniu').reset_index()  #GRUPOWANIE GŁÓWNEGO DF ABY ZLICZYĆ KAŻDEGO PRACOWNIKA NA KAŻDYM ZLECENIU
        count_worker_summary_time = count_worker_summary_time.rename(columns={"Czas na zleceniu" : "Suma pracownika"}) #ZMIANA NAZW KOLUMN NA DOKLADNIEJSZE
        count_worker_summary_time = pd.merge(sorted_df, count_worker_summary_time[['Suma pracownika', 'id', 'Zlecenie']], on=['id','Zlecenie'], how='left') #DO POSORTOWANEGO DF Z LICZBA GODZIN W DANEJ DACIE DOŁĄCZA SUMĘ GODZIN DLA DANEJ OSOBY
        count_worker_summary_time = count_worker_summary_time.drop(columns=['id'])  #KOLUMNA ID NIE BEDZIE JUZ POTRZEBNA
        count_worker_summary_time = count_worker_summary_time[['Zlecenie', 'Nazwisko', 'Imie', 'DataCzas', 'Czas na zleceniu', 'Suma pracownika']]  #REORGANIZACJA KOLUMN KTORE MAJA BYC WYSWIETLANE W DANEJ KOLEJNOSCI
        suma_dnia = suma_dnia.rename(columns={"Czas na zleceniu" : "Suma dnia"}) #ZMIANA NAZW KOLUMN NA DOKŁADNIEJSZE + OMIJA DZIEKI TEMU DUBLOWANIE KOLUMN W PRZYSZŁYCH MERGE'ACH
        count_worker_summary_time = pd.merge(count_worker_summary_time, suma_dnia, on=['DataCzas'], how='left') #DO GŁÓWNEJ ROZPISKI GODZIN KAŻDEGO PRACOWNIKA DORZUCA SUME GODZIN NA DANY DZIEŃ PO KEY=DATACZAS
        count_worker_summary_time = pd.merge(count_worker_summary_time, suma_zlecenia, on=['Zlecenie'], how='left') #DO GŁÓWNEGO DF DORZUCA SUME GODZIN NA CAŁYM ZLECENIU PO KEY=ZLECENIE
        count_worker_summary_time = count_worker_summary_time.sort_values(['DataCzas']) #SORTOWANIE WSTĘPNE KOLUMN, ABY BYŁY POSORTOWANE DATĄ NARASTAJĄCO
        count_worker_summary_time[['Czas na zleceniu', 'Suma zlecenia', 'Suma dnia', 'Suma pracownika']] = count_worker_summary_time[['Czas na zleceniu', 'Suma zlecenia', 'Suma dnia', 'Suma pracownika']].astype(float)
        count_worker_summary_time[['DataCzas']] = count_worker_summary_time[['DataCzas']].astype(str)
        ff = pd.pivot(count_worker_summary_time, index=['Suma zlecenia',  'Zlecenie', 'Nazwisko', 'Imie','Suma pracownika'], columns=['DataCzas','Suma dnia'])  #STWORZENIE TABELO Z MULTIINDEXAMI
        ff.fillna(value = "", inplace = True, axis= 1)      #WYPEŁNIA PUSTE POLA PUSTYM STRINGIEM W CELU LEPSZEJ PRZEJRZYSTOŚCI TABELI
        ff = ff.sort_index(level=[1, 2])            # SORTOWANIE ZLECENIEM A NASTĘPNIE NAZWISKIEM
        final = ff.to_html(justify="center", classes="data-1 table table-hover")
        
    if request.method == 'GET':
        message = "Click to export report to excel (your download folder)"
        context = {
        'final' : final,
        'message' : message,
        }
        return render(request, template_name, context)
    if request.method == 'POST':
        file_name = query_piece
        if "/" in file_name:
            file_name = file_name.replace('/', '-')
        download_folder = os.path.expanduser("~")+"/Downloads/"
        passed_df = ff.to_excel(excel_writer = download_folder + file_name + ' - excel_report.xlsx', float_format = '%2f', index=True, encoding = 'utf-8', header=True)
        passed_df
        excel = Dispatch('Excel.Application')      
        wb = excel.Workbooks.Open(download_folder + file_name + ' - excel_report.xlsx')
        excel.Worksheets(1).Activate()
        excel.ActiveSheet.Columns.AutoFit()
        excel.ActiveWindow.Zoom = 85
        wb.Save()
        wb.Close()
        message = "Excel generated at your local 'Downloads' folder"
        context = {
        'final' : final,
        'message' : message,
        }
        return render(request, template_name, context)

def order_view(request, template_name ='y_day/order_view.html'):    #Widok odpowiedzialny za wyświetlenie formularza do wprowadzenia numeru zlecenia bądź numeru/etap zlecenia
    form = InputOrderForm()
    context = {
        'form': form
    }
    if 'input_order' in request.POST:
        form = InputOrderForm(request.POST)
        order_number = form['input_order']
        order_number = str(order_number.value())
        if '/' in order_number:
            order_number = order_number.replace("/", "#2F")
        if form != '':
            return redirect('order_details', order=order_number)
    else:
        context = {
            'form': form
        }
        return render(request, template_name, context)