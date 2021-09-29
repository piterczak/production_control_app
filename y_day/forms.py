import datetime
from django import forms
from django.core.exceptions import ValidationError

class InputDateForm(forms.Form):
    fields = ['input_data']
    input_data = forms.DateField(input_formats=['%Y-%m-%d'], label='Wpisz date', localize=True, widget=forms.DateInput(format='%Y-%m-%d', attrs={'type': 'date'}))

    def clean_input_data(self):
        data = self.cleaned_data['input_data']

        #check if date is not in the future
        if data > datetime.date.today():
            raise ValidationError('Błędna data - podano datę w przyszłości!')
        return data

class InputWeekForm(forms.Form):
    input_week = forms.DateField(label="Wpisz datę z której chcesz wyświetlić raport.", widget=forms.DateInput(format=('%Y-W%W'), attrs={'type':'week'}))
    
class InputOrderForm(forms.Form):
    fields = ['input_order']
    input_order = forms.CharField(label="Wpisz numer zlecenia dla którego chcesz wyświetlić raport.")
