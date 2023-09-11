import pandas as pd
import sqlite3

# teria que atualizar todo dia primeiro de cada mês

# Lê o excel da ABECS
df = pd.read_excel(r'C:\Users\Lucas\Desktop\Python\ABECS\Arquivos\31-08-2023.xlsx', names =['CNPJ','MCC','TIPO','DATA'], dtype={'CNPJ' : 'string', 'MCC' : 'string', 'TIPO' : 'string', 'DATA' : 'string'})

# Deleta linhas que não tenham valores provindas do excel
df.dropna(axis=0, how='any', inplace=True)

# Transforma o df em lista
lista = df.values.tolist()

# Cria a tabela na database
connection = sqlite3.Connection(r'C:\Users\Lucas\Desktop\Python\ABECS\Arquivos\depara_abecs.db')
cursor = connection.cursor()

cursor.execute( '''
          CREATE TABLE IF NOT EXISTS MCCS_DETERMINADOS
          ([CNPJ_BANDEIRA_ABECS] TEXT PRIMARY KEY, [MCC_BANDEIRA_ABECS] TEXT, [TIPO_ABECS] TEXT, [DATA_DETERMINACAO_ABECS] TEXT)''')

# Cria dicionário, para usar os CNPJs como key, e assim criar uma lista de MCCs por CNPJ nos values

d = dict()

# Formata os CNPJs para ficarem com 14 dígitos, e adiciona os cnpjs como keys de um dicionário, como citado acima
for e in lista:
    # Formata o CNPJ
    if len(e[0]) < 14:
        e[0] = ((14 - len(e[0])) * '0' ) + e[0]
    # Faz uma verificação : se o CNPJ não está no dicionário, adiciona o MCC determinado a uma lista, o status a uma string, e datas de determinação à outra lista
    if e[0] not in d.keys():
        d[e[0]] = (list(), e[2], list())
        d[e[0]][0].append(e[1])
        d[e[0]][2].append(e[3])
    # Caso este cnpj já esteja na lista, apenas complementa a lista de MCCs determinados e datas de determinação
    else:
        d[e[0]][0].append(e[1])
        d[e[0]][2].append(e[3])
    
# Insere dados do dicionário na table MCCS_DETERMINADOS
# Cnpj é a key do dicionário, e outros dados estão em uma tupla nos values deste dict, desempacota todas as infos necessária e sobe elas para o .db por meio de uma tupla
for e in d.keys():
    cnpj = e # Cnpj
    i = 1
    if len(d[e][0]) == 1: # Se só houver um MCC principal, adiciona ele
        mccs_principais = "".join(d[e][0])
    else: # Caso haja mais de um MCC principal, itera pelos dict.values destes MCCs e adiciona o MCC ao database em uma string como : '1234,5678,....'
        mccs_principais = str(d[e][0][0]) 
        while i < len(d[e][0]):
            mccs_principais += ',' + str(d[e][0][i])
            i += 1 
    tipo = str(d[e][1])
    i = 1
    if len(d[e][2]) == 1: # Se só houver uma data de determinação, adiciona ela
        data = "".join(d[e][2])
    else: # Caso haja mais de uma data de determinação, itera pelos dict.values, compara se as datas são iguais, e depois adiciona ao database como string no formato : 'dd/mm/aaaa 00:00 | dd/mm/aaaa 01:01'
        data = "".join(d[e][2][0]) 
        while i < len(d[e][2]):
            if data == str(d[e][2][i]): # compara se data da determinação 1 == data da determinação 2
                i += 1
            else:
                data += ' | ' + str(d[e][2][i])
                i += 1 
    
    dados = (cnpj, mccs_principais, tipo, data)
    
    cursor.execute('''INSERT OR REPLACE INTO MCCS_DETERMINADOS VALUES (?,?,?,?)''', dados)

connection.commit()