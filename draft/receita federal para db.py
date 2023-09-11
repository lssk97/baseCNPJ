import os
import sqlite3

# criar scraping que verifica a última versão e verifica em um file.txt como data='aabbcccc' e baixa se for mais novo
# criar função que tira o zip dos arquivos

os.chdir(r'C:\Users\Lucas\Downloads\Estabelecimentos_temp')

files = list(os.listdir())

connection = sqlite3.Connection(r'C:\Users\Lucas\Desktop\Python\ABECS\Arquivos\depara_abecs.db')
cursor = connection.cursor()

cursor.execute( '''
          CREATE TABLE IF NOT EXISTS DADOS_RECEITA
          ([CNPJ_RECEITA] TEXT PRIMARY KEY, [SITUACAO_CADASTRAL_RECEITA] TEXT, [CNAE_PRINCIPAL_RECEITA] TEXT, [CNAES_SECUNDARIOS_RECEITA] TEXT)
          ''')

erros = 0
lista_erros = list()
for e in files:
    with open(e, encoding='latin-1') as f:
        print(f'processando arquivo {e}')
        for z in f:
            x = z.split(";")
            cnpj = (x[0] + x[1] + x[2]).replace('"','')
            situacao_cadastral = x[5].replace('"','')
            cnae_p = x[11].replace('"','')
            cnae_s = x[12].replace('"','')
            dados = (cnpj, situacao_cadastral, cnae_p, cnae_s)
            try:
                cursor.execute('''INSERT OR REPLACE INTO DADOS_RECEITA VALUES (?,?,?,?)''', dados)
            except:
                erros += 1
                lista_erros.append(dados)

connection.commit()

print(f'erros = {erros}')
            


