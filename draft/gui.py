import PySimpleGUI as sg
import sqlite3

connection = sqlite3.Connection(r'C:\Users\ls33034\Documents\Scripts offline\virtual_envs\base_receita_federal\base_receita\arquivos\databases\julho_2023.db')
cursor = connection.cursor()

lista_resultados = [['              ','    ','    ','       ','        ']]
table_headers = ['CNPJ','MCC BANDEIRAS','MCC CNAE','CNAE PRIMÁRIO','STATUS CNPJ']

layout = [
[sg.Text('Digite um CNPJ ou Raíz de CNPJ para a busca', key = '-STATUS-')],
[sg.Input('', key = '-INPUT_CNPJ-')],
[sg.Button('CNPJ', key = '-BUTTON_CNPJ-'), sg.Button('Raíz', key = '-BUTTON_RAIZ')],
[sg.Table(lista_resultados, table_headers,justification = 'center', key='-TABELA_RESULTADOS-')],
[sg.Button('LOTE CNPJ', key='-IMPORTAR_LOTE-'), sg.Button('EXPORTAR', key='-EXPORTAR_LOTE-'), sg.Button('COPIAR', key='-COPY-')]]

window = sg.Window('Busca CNPJ', layout)

def query_cnpj(cnpj : list, lista_resultados):
    if len(cnpj) == 1:
        cnpj = cnpj[0]
        q = cursor.execute(f"""
        SELECT 
        CNPJ_RECEITA,
        MCC_BANDEIRA_ABECS,
        MCC_PRINCIPAL_ABECS,
        CNAE_PRINCIPAL_RECEITA,
        
        CASE
            WHEN SITUACAO_CADASTRAL_RECEITA = '01' THEN 'NULA'
            WHEN SITUACAO_CADASTRAL_RECEITA = '02' THEN 'ATIVA'
            WHEN SITUACAO_CADASTRAL_RECEITA = '03' THEN 'SUSPENSA'
            WHEN SITUACAO_CADASTRAL_RECEITA = '04' THEN 'INAPTA'
            WHEN SITUACAO_CADASTRAL_RECEITA = '08' THEN 'BAIXADA'
        ELSE 'NAO IDENTIFICADO'
        END AS DESCRIÇAO_RECEITA

        FROM DADOS_RECEITA

        LEFT JOIN MCCS_DETERMINADOS ON DADOS_RECEITA.CNPJ_RECEITA = MCCS_DETERMINADOS.CNPJ_BANDEIRA_ABECS

        LEFT JOIN DEPARA ON DADOS_RECEITA.CNAE_PRINCIPAL_RECEITA = DEPARA.CNAE_PRINCIPAL_ABECS

        WHERE CNPJ_RECEITA = '{cnpj}'
        -- aqui pode ser = 'cnpj'

        ORDER BY SITUACAO_CADASTRAL_RECEITA
        """).fetchone()

        if q:
            lista_resultados.append([q[0],q[1],q[2],q[3],q[4]])
        else:
            lista_resultados.append([cnpj,'N/A','N/A','N/A','Não encontrado'])
    

def quer_raiz(raiz : str, lista_resultados):
    pass

while True:
    event, values = window.read()
    if event == sg.WIN_CLOSED or event == 'Exit':
        break
    elif event == '-BUTTON_CNPJ-':
        l = list()
        l.append(values['-INPUT_CNPJ-'])
        query_cnpj(l, lista_resultados)
        window['-TABELA_RESULTADOS-'].update(lista_resultados)
        window.refresh()


# cnpj, situação receita, mcc abecs, 
