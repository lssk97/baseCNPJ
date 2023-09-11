import PySimpleGUI as sg
import sqlite3

connection = sqlite3.Connection(r'C:\Users\Lucas\Desktop\Python\ABECS\Arquivos\depara_abecs.db')
cursor = connection.cursor()

lista_resultados = [['              ','     ','    ','    ','     ','       ','       ']]
table_headers = ['CNPJ','STATUS CNPJ','MCC BANDEIRAS','MCC CNAE','MCC SECUNDÁRIO','CNAE PRIMÁRIO', 'CNAE SECUNDÁRIO']

layout = [
[sg.Text('Digite um CNPJ ou Raíz de CNPJ para a busca', key = '-STATUS-')],
[sg.Input('', key = '-INPUT_CNPJ')],
[sg.Button('CNPJ', key = '-BUTTON_CNPJ'), sg.Button('Raíz', key = '-BUTTON_RAIZ')],
[sg.Table(lista_resultados, table_headers,justification = 'center', key='-TABELA_RESULTADOS')],
[sg.Button('LOTE CNPJ', key='-IMPORTAR_LOTE-'), sg.Button('EXPORTAR', key='EXPORTAR_LOTE'), sg.Button('COPIAR', key='-COPY-')]]

window = sg.Window('Busca CNPJ', layout)

def query_cnpj(cnpj, lista_resultados):
    pass



while True:
    event, values = window.read()
    if event == sg.WIN_CLOSED or event == 'Exit':
        break