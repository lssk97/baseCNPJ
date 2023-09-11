import pandas as pd
import sqlite3

# Cria o dataframe do excel de de_para da ABECS
df = pd.read_excel(r'C:\Users\Lucas\Desktop\Python\ABECS\Arquivos\Planilha-DE-PARA-com-MCCs-atualizado-julho-2023.xlsx', header=1, usecols=[3,5,7,9,11,13,15,17,19],\
    names=['CNAE','MCCP','MCC1','MCC2','MCC3','MCC4','MCC5','MCC6','MCC7'])

# Preenche os campos vazios do excel com zero
df = df.fillna(0)

# Transforma o df em uma lista de valores
lista = df.values.tolist()

# Itera pela lista, e insere os dados em uma table do database
connection = sqlite3.Connection(r'C:\Users\Lucas\Desktop\Python\ABECS\Arquivos\depara_abecs.db')
cursor = connection.cursor()

cursor.execute( '''
          CREATE TABLE IF NOT EXISTS DEPARA
          ([CNAE_PRINCIPAL_ABECS] TEXT PRIMARY KEY, [MCC_PRINCIPAL_ABECS] TEXT, [MCCS_SECUNDARIOS_ABECS] TEXT)
          ''')

for e in lista:
    # Separa o CNAE e MCC Primário, prepara uma lista de MCCs Alterantivos
    cnae = e[0].replace("/","").replace("-","")
    mcc = e[1]
    mccs_alternativos = str()
    i = 2
    # Varre os MCCs Alternativos e adiciona eles na lista casos exista algum
    for x in range(7):
        if e[i] != 0:
            if len(mccs_alternativos) == 0:
                mccs_alternativos = mccs_alternativos + f'{str(e[i]).replace(".0","")}'
            else:
                mccs_alternativos = mccs_alternativos + ',' f'{str(e[i]).replace(".0","")}'
        i += 1
    # Adiciona ao database{
    cursor.execute('''INSERT OR REPLACE INTO depara VALUES (?,?,?)''', (cnae,mcc,mccs_alternativos))

connection.commit()

