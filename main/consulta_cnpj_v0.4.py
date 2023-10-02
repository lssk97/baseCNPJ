import pandas as pd
import sqlite3
import PySimpleGUI as sg
import wget
import os
import shutil
from zipfile import ZipFile
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException # Elemento não encontrado
from selenium.common.exceptions import NoSuchDriverException # Driver não está no path direcionado pelo Service
from selenium.common.exceptions import SessionNotCreatedException # Driver muito antigo
from selenium.common.exceptions import WebDriverException # Sem internet
from selenium.webdriver import ChromeService  
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import time

# by Lucas Staub
# finalizado em xx/xx/xxxx

# Lógica do programa:
# Busca nas fontes de dados por webscraping/selenium (dados abertos da receita federal e site da ABECS) os arquivos para criar uma database, que servirá como fonte de consulta por SQL
# Faz verificação a cada abertura do programa, comparando as versões dos dados da receita e abecs com os vigentes no database (versão vigente é escrita no config.txt)
# Caso hajam novos dados, formata e baixa estes dados, e atualiza o database local
# Utiliza uma GUI para consultas de SQL do database local, pro querido Analista de Pricing usar

# Limitações:
# É necessário um webdriver atualizado para fazer o webscraping, ou o programa não funciona
# Pode haver defasagem de dados, uma vez que:
    # Base da Receita Federal é atualizada uma vez por mês

# Quantidade de dados gera um arquivo .db de aproximadamente 4gb

# Dá pra reduzir algumas funções e deixar o script melhor, mas isso fica para uma v2 (espero q dê tempo)
# Se der problema no código em algum momento, mucho sorry, isso aqui é um puxadinho enquanto a API da ABECS não integra nos sistemas da Get daqui a 10 anos

class Dados():
    '''Classe para buscar, atualizar e tratar dados das diferentes fontes'''
    def __init__(self):
        '''Cria variáveis de versionamentos dos dados'''

        ''' Versões vigentes dos arquivos são armazenados em um dict, lidos do 'config.txt'
        Valores deste dict é uma tupla, com versão lida do file inicial, e versão atual lida pelo webscraping
        Se valores da tupla forem diferentes, atualiza o database e sobrescreve a nova versão no .txt ao finalizar o update
        Keys são sempre: {'receita', 'depara_abecs', 'lista_cnpj'}'''
        self.current_versions = dict()
        self.current_versions['receita'] = []
        self.current_versions['depara_abecs'] = []
        self.current_versions['lista_cnpj'] = []
        self.update = True
        self.path_script = os.path.abspath(os.path.dirname(__file__))
        self.path_temp = self.path_script + '\\temp'
        self.connection = sqlite3.Connection(os.path.join(self.path_script,'database.db'))
        self.cursor = self.connection.cursor()
    

    def read_version(self):
        '''Lê no arquivo config.txt quais são as versões/data dos dados da Receita e ABECS
        Se o arquivo ainda não existir, cria ele com versões vazias'''

        try: 
            with open('config.txt', mode='r') as file:
                # Arquivo config.txt já existe
                i = 0
                while i < 3:
                    x = str(file.readline()).split("=")
                    self.current_versions[x[0]].append(x[1].replace('\n',''))
                    i += 1
                file.close()
        except FileNotFoundError:
            # Se arquivo não existe, cria ele
            with open('config.txt', mode='w') as file:
                file.write('receita=nula\n')
                file.write('depara_abecs=nula\n')
                file.write('lista_cnpj=nula\n')
                file.write('permite_update=Sim')
                file.close()


    def cria_database(self):
        '''Inicializa database e table se elas ainda não existirem'''
        
        # Receita Federal
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS DADOS_RECEITA
        ([CNPJ_RECEITA] TEXT PRIMARY KEY, [SITUACAO_CADASTRAL_RECEITA] TEXT, [CNAE_PRINCIPAL_RECEITA] TEXT, [CNAES_SECUNDARIOS_RECEITA] TEXT)
        ''')
        
        # Lista de MCCs determinados pelas bandeiras
        self.cursor.execute( '''
        CREATE TABLE IF NOT EXISTS MCCS_DETERMINADOS
        ([CNPJ_BANDEIRA_ABECS] TEXT PRIMARY KEY, [MCC_BANDEIRA_ABECS] TEXT, [TIPO_ABECS] TEXT, [DATA_DETERMINACAO_ABECS] TEXT)''')
        
        # Relação de CNAE/MCC da ABECS
        self.cursor.execute( '''
        CREATE TABLE IF NOT EXISTS DEPARA
        ([CNAE_PRINCIPAL_ABECS] TEXT PRIMARY KEY, [MCC_PRINCIPAL_ABECS] TEXT, [MCCS_SECUNDARIOS_ABECS] TEXT)
        ''')
        
        self.connection.commit()


    def permite_update(self):
        '''Configuração vinculada ao self.update, que determina se a database pode ser atualizada ou não.
        Variável nasce como True, e se estiver como 'Sim' no arquivo de configurações, vai fazer scraping e updates.
        Se estiver zerada ou como flag 'não', ficam estáticos os valores da DB.
        Configuração existe pois o site da receita fica lento para donwloads nos primeiros dias de update dos dados de CNPJ.
        Configurável pela GUI'''
        
        try: 
            with open('config.txt', mode='r+') as file:
                # Arquivo config.txt já existe, lê preferência do user
                update = file.readlines()[3].split('=')[1]
                # Várias opções de sim caso o user digite errado
                if update in ('Sim','sim','S','s'):
                    self.update = True
                else:
                    self.update = False
                file.close()
        except FileNotFoundError:
            # Se o arquivo não existe, cria ele
            with open('config.txt', mode='w') as file:
                file.write('receita=nula\n')
                file.write('depara_abecs=nula\n')
                file.write('lista_cnpj=nula\n')
                file.write('permite_update=sim')
                file.close()

        
    def update_download_receita(self):
        '''Se a configuração permitir updates, e a versão vigente dos dados for diferente da encontrada no webscraping,
        cria uma pasta temporária para os downloads dos novos arquivos
        
        Verifica se todos os arquivos .zip já foram baixados da receita.
        Enquanto houverem arquivos Estabelecimentos(n).zip para baixar, continua tentando pelo wget.
        Depois de todos os files baixados, descompacta todos eles''' 
        
        if self.update and (self.current_versions['receita'][0] != self.current_versions['receita'][1]):
            '''Cria pasta de arquivos temporários se ela não existir'''
            if not os.path.isdir(self.path_temp):
                os.mkdir(self.path_temp)

            '''Deleta arquivos .tmp de jobs de download que possam ter falhado anteriormente'''
            for file in os.listdir(os.path.join(self.path_script,'temp\\')):
                if file.endswith(".tmp"):
                    os.remove(os.path.join(self.path_script,f'temp\\{file}'))
            
            files = 'https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos'

            '''Verifica se todos os .zip já foram baixados, se não, baixa eles'''

            print(f'''--------------------------------------------------------------------------------
Arquivos da receita devem ser atualizados!
A versão atual da base é {self.current_versions["receita"][0]},
Há uma nova versão: {self.current_versions["receita"][1]}
--------------------------------------------------------------------------------
Se você não quiser atualizar a base, leia o procedimento no Confluence:
'link do confluence'

Ou feche o programa, vá até a pasta da sua instalação
Abra a pasta 'dist' e procure pelo arquivo 'config.txt'
Substitua:
permite_update=sim
por
permite_update=nao
Salve o arquivo
Abre novamente o programa e ele não irá atualizar as bases
--------------------------------------------------------------------------------
Para reativar as atualizações das bases
no 'config.txt' repita os passos acima, mas deixe como:
permite_update=sim
--------------------------------------------------------------------------------
Esse update demora alguma hora ou outra, fique à vontade para pegar um café =)
Geralmente são 10 arquivos a serem baixados da Receita
Se você interromper o processo de download, ele retoma a partir do último arquivo baixado
Não se preocupe se você precisar fechar o programa e baixar os dados outra hora!
--------------------------------------------------------------------------------
obs:
Se a base da Receita Federal tiver sido atualizada há um ou dois dias atrás, os downloads devem demorar mais que o normal
Se esse for o caso, recomendo esperar alguns dias e travar os updates, como detalhado mais acima
--------------------------------------------------------------------------------''')
            
            # Usa replace pq não pode '/' em nome de arquivos!
            data_arquivos_receita = self.current_versions["receita"][1].replace("/","_")
            faltam_arquivos = True
            i = 0
            while faltam_arquivos == True:
                # Se o arquivo Estabelecimento(n)_data_arquivo existe, itera para o próximo download
                if os.path.isfile(os.path.join(self.path_script,f'temp\\Estabelecimentos{i}_{data_arquivos_receita}.zip')) \
                or os.path.isfile(os.path.join(self.path_script,f'temp\\Estabelecimentos{i}_{data_arquivos_receita}.ESTABELE')):
                    i += 1
                    print(f'O {i}º arquivo da receita já foi baixado')
                else:
                    # Se ele não existe, tenta baixar até acabar o total de arquivos disponíveis na receita federal
                    try:
                        print(f'''
Baixando o {(i+1)}º arquivo da receita''')
                        wget.download(url=f'{files}{i}.zip', out=os.path.join(self.path_script,f'temp\\Estabelecimentos{i}_{data_arquivos_receita}.zip'))
                        i += 1
                    except Exception as e:
                    # Se não conseguir baixar ou não houver arquivos para baixar, ou outros erros, fecha o loop
                        print(f'Arquivo {i+1} = {e} ou não existe')
                        faltam_arquivos = False
            
            ''' Checka os arquivos .zip
            Se o arquivo já tiver sido descompactado, deleta o arquivo zipado
            Se não, extrai e deleta o file zipado equivalente'''
            
            # Olha todos os arquivos na pasta temporária
            for file in os.listdir(os.path.join(self.path_script,'temp')):
                full_path_file = os.path.join(self.path_script,f'temp\\{file}')
                # Se o arquivo for zip, vê se existe um arquivo equivalente .ESTABELE já extraído, se sim, exclui o .zip
                if file.endswith(".zip"):
                    if file[:-4] + '.ESTABELE' in os.listdir(os.path.join(self.path_script,'temp')):
                        os.remove(os.path.join(self.path_script,f'temp\\{file}'))
                    # Se não tiver sido extraído ainda, extrai, renomeia para o mesmo nome mas com a extensão .ESTABELE e deleta o arquivo .zip
                    else:
                        with ZipFile(full_path_file) as arquivozip:
                            zip0 = arquivozip.infolist()[0]
                            zip0.filename = file[:-4] + '.ESTABELE'
                            arquivozip.extract(zip0, path=os.path.join(self.path_script,f'temp\\'))
                            print(f'Extraindo o arquivo {zip0.filename}')
                        os.remove(os.path.join(self.path_script,f'temp\\{file}'))


    def atualizar_receita(self):
        '''Para cada um dos arquivos .ESTABELE na pasta \temp, pega os campos relevantes:
        CNPJ, CNAE_PRIMÁRIO, CNAE SECUNDÁRIO E SITUAÇÃO NA RECEITA
        Insere no database, dando drop no anterior'''
        
        print('''--------------------------------------------------------------------------------
Os arquivos serão inseridos no banco de dados, por favor, não feche o programa
--------------------------------------------------------------------------------''')

        # Dropa a table anterior de dados da receita e recria ela
        # Evita erros de registro
        self.cursor.execute('''DROP TABLE DADOS_RECEITA''')
        
        self.connection.commit()
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS DADOS_RECEITA
        ([CNPJ_RECEITA] TEXT PRIMARY KEY, [SITUACAO_CADASTRAL_RECEITA] TEXT, [CNAE_PRINCIPAL_RECEITA] TEXT, [CNAES_SECUNDARIOS_RECEITA] TEXT)
        ''')

        # Lê lista de arquivos .ESTABELE 
        estabele = list()
        for file in os.listdir(os.path.join(self.path_script,'temp\\')):
            if file.endswith('.ESTABELE'):
                estabele.append(os.path.join(self.path_script,f'temp\\{file}'))
                
        # Retira os dados desnecessários dos arquivos e insere na table
        # Conta quantos registros foram inseridos, e se houveram erros
        count_lines = 0
        erros = 0 # seria melhor um dict com o nome do arquivo e quantidade de erros como value, pero habemus preguiça
        for e in estabele:
            print(f'Inserindo o arquivo {e} na base de dados')
            with open(e, mode='r', encoding='latin-1') as arq:
                for z in arq.readlines():
                    count_lines += 1
                    try:
                        x = z.split(";")
                        cnpj = (x[0] + x[1] + x[2]).replace('"','')
                        situacao_cadastral = x[5].replace('"','')
                        cnae_p = x[11].replace('"','')
                        cnae_s = x[12].replace('"','')
                        dados = (cnpj, situacao_cadastral, cnae_p, cnae_s)
                        self.cursor.execute('''INSERT OR REPLACE INTO DADOS_RECEITA VALUES (?,?,?,?)''', dados)
                    except:
                        erros += 1

        '''Dá commit() no SQL e testa se há o mesmo número de registro no database que linhas nos arquivos
        Se estiver funcionando, apaga todos os arquivos baixados anteriormente
        Atualiza config '''
        
        print(f'''--------------------------------------------------------------------------------
Linhas com erros que não foram inseridas: {erros}
Quantidade de linhas nos arquivos da receita: {count_lines}
--------------------------------------------------------------------------------''')
        self.connection.commit()
        z = self.cursor.execute('''SELECT COUNT(*) FROM DADOS_RECEITA''').fetchone()  
        if z:
            print(f'Quantidade de linhas no database: {z}')
            print(f'Diferença de linhas dos arquivos e registros no database: {count_lines - z[0]}')
            # Deleta pasta \temp
            shutil.rmtree(os.path.join(self.path_script,'temp\\'))
            with open('config.txt', mode='r') as read:
                atual = read.readlines()
                nova_data_update = f'receita={self.current_versions["receita"][1]}\n'
                atual[0] = nova_data_update
                read.close()
                with open('config.txt', mode='w') as write:
                    write.writelines(atual)
            print('''Base da Receita Federal atualizada!
--------------------------------------------------------------------------------''')
        # Se a query com o CNPJ da Get falhar, não excluí os arquivos da pasta \temp, para rodar o processo outra vez           
        else:
            print('Erro ao subir os arquivos do database, tente novamente')


    def update_cnpjs_abecs(self):
        '''Se a configuração permitir updates, atualiza a relação de CNPJs determinados por bandeira da ABECS
        Verifica se o arquivo .xlsx já existe, se não baixa por wget e sobe no banco de dados
        Ao fim do processo exclui o excel e atualiza o .config'''
        
        if self.update and (self.current_versions['lista_cnpj'][0] != self.current_versions['lista_cnpj'][1]):
            # Link é obtido pelo selenium, pelo href
            # Nome do excel é a última parte do link
            nome_excel = self.current_versions['lista_cnpj'][1].split('/')[-1]
            link = self.current_versions['lista_cnpj'][1]
            
            print(f'''--------------------------------------------------------------------------------
Arquivos de CNPJ Determinados da ABECS receita deve ser atualizado!
A versão atual da base é {self.current_versions["lista_cnpj"][0]},
Há uma nova versão: {self.current_versions["lista_cnpj"][1]}
--------------------------------------------------------------------------------''')
            
            # Se o arquivo ainda não foi baixado, faz download dele
            if not os.path.isfile(os.path.join(self.path_script, nome_excel)):
                wget.download(link)

            file = os.path.join(self.path_script, nome_excel)
            
            # Cria dataframe do excel e formata dados
            df = pd.read_excel(file, names=['CNPJ','MCC','TIPO','DATA'],usecols=[0,1,2,3], dtype={'CNPJ' : 'string', 'MCC' : 'string', 'TIPO' : 'string', 'DATA' : 'string'})
            df.dropna(axis=0, how='any', inplace=True)
            lista = df.values.tolist()

            # Cria dicionário, para usar os CNPJs como key, e assim criar uma lista de MCCs por CNPJ nos values, evitando duplicados
            d = dict()

            # Formata o CNPJ
            for e in lista:
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

            # Insere dados do dicionário na table MCCS_DETERMINADOS, dropando os dados anteriores
            # Cnpj é a key do dicionário, e outros dados estão em uma tupla nos values deste dict, desempacota todas as infos necessária e sobe elas para o .db por meio de uma tupla

            self.cursor.execute('''DROP TABLE MCCS_DETERMINADOS''')
            
            self.connection.commit()
        
            self.cursor.execute( '''
            CREATE TABLE IF NOT EXISTS MCCS_DETERMINADOS
            ([CNPJ_BANDEIRA_ABECS] TEXT PRIMARY KEY, [MCC_BANDEIRA_ABECS] TEXT, [TIPO_ABECS] TEXT, [DATA_DETERMINACAO_ABECS] TEXT)
            ''')
         
            for e in d.keys():
                cnpj = e
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

                self.cursor.execute('''INSERT OR REPLACE INTO MCCS_DETERMINADOS VALUES (?,?,?,?)''', dados)

            self.connection.commit()

            # Verifica se funcionou o update
            z = self.cursor.execute('''SELECT COUNT(*) FROM MCCS_DETERMINADOS''').fetchone()  
            if z:
                print(f'''
Quantidade de MCCs determinados atualizados: {z}''')
            else:
                print('''Verificar código: Não foi possível fazer update dos CNPJs determidos
Avise o responsável pela automação''')

            # Terminado a inserção, exclui o excel e atualiza o file .config
            os.remove(file)
            with open('config.txt', mode='r') as read:
                atual = read.readlines()
                novo_link_update = f'lista_cnpj={self.current_versions["lista_cnpj"][1]}\n'
                atual[2] = novo_link_update
                read.close()
                with open('config.txt', mode='w') as write:
                    write.writelines(atual)
            print('''CNPJs Determinados atualizados!
--------------------------------------------------------------------------------''')

            

    def update_depara_abecs(self):
        '''Se a configuração permitir updates, atualiza a relação de:para da ABECS
        Verifica se o arquivo .xlsx já existe, se não baixa por wget e sobe no banco de dados
        Ao fim do processo exclui o excel e atualiza o .config'''

        if self.update and (self.current_versions['depara_abecs'][0] != self.current_versions['depara_abecs'][1]):
            # Link é obtido pelo selenium, pelo href
            # Nome do excel é a última parte do link
            nome_excel = self.current_versions['depara_abecs'][1].split('/')[-1]
            link = self.current_versions['depara_abecs'][1]
            
            print(f'''--------------------------------------------------------------------------------
Arquivos de DE:PARA CNAE da ABECS receita deve ser atualizado!
A versão atual da base é {self.current_versions["depara_abecs"][0]},
Há uma nova versão: {self.current_versions["depara_abecs"][1]}
--------------------------------------------------------------------------------''')
            
            # Se o arquivo ainda não foi baixado, faz download dele
            if not os.path.isfile(os.path.join(self.path_script, nome_excel)):
                wget.download(link)

            file = os.path.join(self.path_script, nome_excel)

            # Cria o dataframe do excel de de_para da ABECS
            # É possível o CNAE ter até 7 MCCs alternativos // se tiver mais no futuro, alterar o código
            df = pd.read_excel(file, header=1, usecols=[3,5,7,9,11,13,15,17,19],\
                               names=['CNAE','MCCP','MCC1','MCC2','MCC3','MCC4','MCC5','MCC6','MCC7'])
            df = df.fillna(0)
            lista = df.values.tolist()
            
            # Itera pela lista, e insere os dados em uma table do database, dropando os valores anteriores

            self.cursor.execute('''DROP TABLE DEPARA''')
            
            self.connection.commit()

            self.cursor.execute( '''
            CREATE TABLE IF NOT EXISTS DEPARA
            ([CNAE_PRINCIPAL_ABECS] TEXT PRIMARY KEY, [MCC_PRINCIPAL_ABECS] TEXT, [MCCS_SECUNDARIOS_ABECS] TEXT)
            ''')

            for e in lista:
                # Separa o CNAE e MCC Primário
                cnae = e[0].replace("/","").replace("-","") # Tira os '-' '/' do cnae
                mcc = e[1]
                mccs_alternativos = str()
                i = 2 # Item e[2] da lista são os MCCs alternativos, que serão iterados abaixo
                # Varre os MCCs Alternativos e adiciona eles na string casos exista algum
                for x in range(7):
                    if e[i] != 0: # Se houver mcc_alternativo
                        if len(mccs_alternativos) == 0:
                            mccs_alternativos = mccs_alternativos + f'{str(e[i]).replace(".0","")}' # Caso seja o primero MCC alternativo, adiciona na string
                        else:
                            mccs_alternativos = mccs_alternativos + ',' f'{str(e[i]).replace(".0","")}' # Se já houver um MCC alternativo, adiciona uma vírgula e o MCC na string
                self.cursor.execute('''INSERT OR REPLACE INTO DEPARA VALUES (?,?,?)''', (cnae,mcc,mccs_alternativos))
                i += 1
                
            self.connection.commit()

            # Verifica se funcionou o update
            z = self.cursor.execute('''SELECT COUNT(*) FROM DEPARA''').fetchone()  
            if z:
                print(f'''
Quantidade de CNAEs atualizados: {z}''')
            else:
                print('''Verificar código: Não foi possível fazer update do DE:PARA ABECS
Avise o responsável pela automação''')

            # Terminado a inserção, exclui o excel e atualiza o file .config
            os.remove(file)
            
            with open('config.txt', mode='r') as read:
                atual = read.readlines()
                novo_link_update = f'depara_abecs={self.current_versions["depara_abecs"][1]}\n'
                atual[1] = novo_link_update
                read.close()
                with open('config.txt', mode='w') as write:
                    write.writelines(atual)
            print('De para atualizado!')        

class GUI():
    '''Interface gráfica do buscador de CNPJs'''

    def __init__(self):
        # Cabeçalho da tabela de resultados da GUI
        self.table_headers = ['CNPJ','STATUS CNPJ','MCC BANDEIRAS','MCC CNAE PRIMÁRIO','CNAE PRIMÁRIO', 'CNAE SECUNDÁRIO']
        # Lista de resultados da GUI, é iniciada assim para a interface ficar com a largura certa
        self.lista_resultados = [['              ','      ','    ','    ','       ','         ']]
        # Layout principal da GUI
        self.layout_main_gui = [
[sg.Text('Digite um CNPJ ou Raíz de CNPJ para a busca:', key = '-MENSAGEM-')],
[sg.Input('', key = '-INPUT_CNPJ-')],
[sg.Text('', key='-STATUS-')],
[sg.Button('CNPJ', key = '-BUTTON_CNPJ-'), sg.Button('Raíz', key = '-BUTTON_RAIZ-')],
[sg.Table(self.lista_resultados, self.table_headers, justification = 'center', key='-TABELA_RESULTADOS-')],
[sg.Button('LOTE CNPJ', key='-IMPORTAR_LOTE-'), sg.Button('EXPORTAR', key='-EXPORTAR_LOTE-'), sg.Button('LIMPAR TABELA', key='-LIMPAR-')]]
        # Janela
        self.window = sg.Window('Busca CNPJ', self.layout_main_gui, resizable=True)
        # Status e avisos da GUI
        self.status = ''
        self.path_script = os.path.abspath(os.path.dirname(__file__))
        self.connection = sqlite3.Connection(os.path.join(self.path_script,'database.db'))
        self.cursor = self.connection.cursor()
        self.lista_cnpjs = []
               
    def main_loop(self):
        '''Loop da janela do PySimpleGUI'''
        
        while True:
            event, values = self.window.read()
            self.window.refresh()
            if event == sg.WIN_CLOSED or event == 'Exit':
                break
            if event == '-BUTTON_CNPJ-':
                self.query_cnpj(values['-INPUT_CNPJ-'])
                self.force_update()
            if event == '-BUTTON_RAIZ-':
                self.query_raiz(values['-INPUT_CNPJ-'])
                self.force_update()
            if event == '-IMPORTAR_LOTE-':
                self.importar_dados_excel()
            if event == '-EXPORTAR_LOTE-':
                self.exportar_dados_excel()
            if event == '-LIMPAR-':
                self.limpa_lista_resultados()


    def limpa_lista_resultados(self):
        '''Zera a lista de resultados'''
        self.lista_resultados = []
        self.status = 'Dados da tabela excluídos'
        self.window['-TABELA_RESULTADOS-'].update(self.lista_resultados)
        self.window['-STATUS-'].update(self.status)
        

    def force_update(self):
        '''Dá update nos dados da GUI'''
        self.window['-TABELA_RESULTADOS-'].update(self.lista_resultados)
        self.window['-STATUS-'].update(self.status)
        self.window.refresh()
                  
        
    def valida_cnpj(self, cnpj : str) -> str:
        '''Formata o input do CNPJ e valida conforme regra da Receita Federal

        returns
        --------
        mensagens de erro : str
            Caso retorne uma str, é para usar na janela como erro
        str(x) : str
            CNPJ Validado'''
    
        cnpj = str(cnpj)
        formatado = f'{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}'
    
        # Valida para verificar se o caractere é um número
        for e in cnpj:
            if e not in {'1','2','3','4','5','6','7','8','9','0'}:
                # .replace tudo que não esta no set válido
                cnpj = cnpj.replace(e,'') 

        # Adiciona zeros à esquerda se estiverem faltando valores, muito comum de dados do Excel/Tableau etc
        if len(cnpj) < 14:
            cnpj = ((14 - len(cnpj)) * '0') + cnpj
    
        if len(cnpj) > 14:
            cnpj = cnpj[:14]
            
        # Regra abaicnpjo provêm de: 'https://www.geradorcnpj.com/algoritmo_do_cnpj.htm' (Verificação dos dois últimos dígitos conforme regra da RF)
        if len(cnpj) == 14: 
            digito_1_list1 = list(cnpj[:12])
            digito_1_list2 = [5,4,3,2,9,8,7,6,5,4,3,2]
            i = 0
            soma = 0

            for e in digito_1_list1, digito_1_list2:
                while i < 12:
                    soma += int(digito_1_list1[i]) * digito_1_list2[i]
                    i += 1

            if soma % 11 < 2:
                digito_1 = str(0)
            else:
                digito_1 = str(11 - (soma % 11))
            digito_2_list1 = list(cnpj[:13])
            digito_2_list2 = [6,5,4,3,2,9,8,7,6,5,4,3,2]
            i = 0
            soma = 0

            for e in digito_2_list1, digito_2_list2:
                while i < 13:
                    soma += int(digito_2_list1[i]) * int(digito_2_list2[i])
                    i += 1

            if soma % 11 < 2:
                digito_2 = str(0)
            else:
                digito_2 = str(11 - (soma % 11))

            # Status final
            formatado = f'{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}'
            if digito_1 != (cnpj[12]) or digito_2 != (cnpj[13]):
                self.status = f'CNPJ {formatado} é inválido'
                return
            elif cnpj in {'00000000000000','11111111111111','22222222222222','33333333333333','44444444444444','55555555555555','66666666666666','77777777777777','88888888888888','99999999999999'}:
                self.status = f'CNPJ {formatado} é inválido'
                return
            else:
                # Se for validado, retorna o CNPJ formatado só com números
                self.status = f'CNPJ {formatado} OK'
                return cnpj

            self.force_update()
            


    def query_cnpj(self, cnpj : str) -> tuple:
        '''Busca na base de dados o CNPJ inputado na GUI, validando se é um CNPJ mesmo e depois rodando a query'''
        
        cnpj = self.valida_cnpj(cnpj)
        if cnpj:
            x = self.cursor.execute(f"""
SELECT CNPJ_RECEITA,

CASE
    WHEN SITUACAO_CADASTRAL_RECEITA = '01' THEN 'NULO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '02' THEN 'ATIVO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '03' THEN 'SUSPENSO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '04' THEN 'INAPTO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '08' THEN 'BAIXADO'
    ELSE 'SITUAÇÃO NÃO ENCONTRADA'
END AS SITUACAO_NOMINAL,
MCC_BANDEIRA_ABECS,
MCC_PRINCIPAL_ABECS,
CNAE_PRINCIPAL_RECEITA,
CNAES_SECUNDARIOS_RECEITA

FROM DADOS_RECEITA 

LEFT JOIN MCCS_DETERMINADOS
ON DADOS_RECEITA.CNPJ_RECEITA = MCCS_DETERMINADOS.CNPJ_BANDEIRA_ABECS

LEFT JOIN DEPARA
ON DADOS_RECEITA.CNAE_PRINCIPAL_RECEITA = DEPARA.CNAE_PRINCIPAL_ABECS

WHERE CNPJ_RECEITA = '{cnpj}'""").fetchone()

            if x:
                self.lista_resultados.append(x)
            else:
                self.lista_resultados.append([cnpj,'CNPJ não encontrado','','','',''])
        

    def query_raiz(self, raiz : str) -> tuple:
        '''Busca na base de dados baseado na raiz do CNPJ'''
        
        # Retira itens que não são numéricos da raiz
        if raiz:
            for e in raiz:
                if e not in {'0','1','2','3','4','5','6','7','8','9','10'}:
                    raiz = raiz.replace(e,'')
        
            # Ajusta lenght da raiz
            if len(raiz) > 8:
                raiz = raiz[:8]
            if len(raiz) < 8:
                raiz = ((len(raiz) - 8) * '0') + raiz
            
        
            x = self.cursor.execute(f"""
SELECT CNPJ_RECEITA,

CASE
    WHEN SITUACAO_CADASTRAL_RECEITA = '01' THEN 'NULO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '02' THEN 'ATIVO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '03' THEN 'SUSPENSO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '04' THEN 'INAPTO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '08' THEN 'BAIXADO'
    ELSE 'SITUAÇÃO NÃO ENCONTRADA'
END AS SITUACAO_NOMINAL,

MCC_BANDEIRA_ABECS,
MCC_PRINCIPAL_ABECS,
CNAE_PRINCIPAL_RECEITA,
CNAES_SECUNDARIOS_RECEITA

FROM DADOS_RECEITA 

LEFT JOIN MCCS_DETERMINADOS
ON DADOS_RECEITA.CNPJ_RECEITA = MCCS_DETERMINADOS.CNPJ_BANDEIRA_ABECS

LEFT JOIN DEPARA
ON DADOS_RECEITA.CNAE_PRINCIPAL_RECEITA = DEPARA.CNAE_PRINCIPAL_ABECS

WHERE CNPJ_RECEITA LIKE '{raiz}%'

ORDER BY SITUACAO_NOMINAL""").fetchall()

            if x:
                for z in x:
                    self.lista_resultados.append(z)
            else:
                self.lista_resultados.append([raiz,'Raíz não encontrada','','','',''])

            self.status = f'Raíz {raiz[:3]+ "." + raiz[3:6] + "." + raiz[6:8]} buscada'
        self.force_update()


    def query_massiva(self, lista : list) -> tuple:
        '''Faz busca de vários CNPJs na query'''

        for cnpj in lista:
            cnpj = self.valida_cnpj(cnpj)
            if cnpj:
                x = self.cursor.execute(f"""
SELECT CNPJ_RECEITA,

CASE
    WHEN SITUACAO_CADASTRAL_RECEITA = '01' THEN 'NULO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '02' THEN 'ATIVO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '03' THEN 'SUSPENSO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '04' THEN 'INAPTO'
    WHEN SITUACAO_CADASTRAL_RECEITA = '08' THEN 'BAIXADO'
    ELSE 'SITUAÇÃO NÃO ENCONTRADA'
END AS SITUACAO_NOMINAL,
MCC_BANDEIRA_ABECS,
MCC_PRINCIPAL_ABECS,
CNAE_PRINCIPAL_RECEITA,
CNAES_SECUNDARIOS_RECEITA

FROM DADOS_RECEITA 

LEFT JOIN MCCS_DETERMINADOS
ON DADOS_RECEITA.CNPJ_RECEITA = MCCS_DETERMINADOS.CNPJ_BANDEIRA_ABECS

LEFT JOIN DEPARA
ON DADOS_RECEITA.CNAE_PRINCIPAL_RECEITA = DEPARA.CNAE_PRINCIPAL_ABECS

WHERE CNPJ_RECEITA = '{cnpj}'""").fetchone()

                if x:
                    self.lista_resultados.append(x)
                else:
                    self.lista_resultados.append([cnpj,'CNPJ não encontrado','','','',''])

    def importar_dados_excel(self) -> list:
        '''Importa dados de lista de CNPJs do Excel pelo pandas'''
        try:
            self.excel_in = sg.popup_get_file('Escolha um arquivo Excel:',
                               title = 'Busca em Lote',
                               default_extension = '.xlsx',
                               keep_on_top = True,
                               no_titlebar = False
                               )
            df = pd.read_excel(self.excel_in, header = None)
            lista = df.values.tolist()
            for e in lista:
                self.query_cnpj(e)
            self.status = 'Busca em lote realizada'
        except Exception as e:
            print(e)
            self.status = 'Não foi possível processar o arquivo em lote'
        
        self.force_update()


    def exportar_dados_excel(self):
        '''Exporta lista de resultados'''
        try:
            self.excel_out = sg.popup_get_file('Salvar os resultados em excel:',
                               title = 'Tabela de valores',
                               save_as = True,
                               default_extension = '.xlsx',
                               keep_on_top = True,
                               no_titlebar = False)

            pd.DataFrame(self.lista_resultados).to_excel(self.excel_out, index=False, header=self.table_headers, engine = 'openpyxl')
            self.status = 'Dados exportados'
        except Exception as e:
            print(e)
            self.status = 'Não foi possível processar o exportar os resultados para Excel'
            
        self.force_update()
  
        
class Browser():
    '''Webdriver'''
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.service = webdriver.ChromeService()
        self.aberto = False
        self.driver_atualizado = False

             
    def add_options(self):
        '''Adiciona os argumentos de opções do browser'''
        self.options.add_argument('--headless')


    def versao_driver(self) -> list:
        '''Verifica a versão do Chrome na máquina e qual a versão do ChromeDriver
        
        returns
        -------
        versao : list
            Lista com a versão do chrome [0] e versão do chromedriver [1]'''
            
        # Pega o user de rede
        user = os.environ['USERNAME']

        # Lê a versão atual do Navegador Google Chrome
        path_versao = f'C:\\Users\\{user}\\AppData\\Local\\Google\\Chrome\\User Data\\Last Version'

        with open(path_versao) as r:
            versao_do_chrome = r.readline()
            r.close()

        # Lê a versão do ChromeDriver pelo cmd
        try:
            versao_driver = os.popen('cmd /c "chromeDriver -v"').read()[12:26]
            if versao_driver == '':
                versao_driver = 'ChromeDriver não está instalado'
        except Exception as error:
            versao_driver = 'Nulo'
            
        return [versao_do_chrome, versao_driver]


    def abrir_navegador(self) -> bool:
        '''Tenta abrir o navegador, se consegue, altera self.aberto para True
        Se o webdriver não conseguir ser aberto, dá print na causa e mantém self.aberto como False'''
        
        try:
            browser.add_options()
            versao = browser.versao_driver()
            print(f'''Versão do Navegador Chrome = {versao[0]}
Versão do ChromeDriver = {versao[1]}
--------------------------------------------------------------------------------''')
            self.navegador = webdriver.Chrome(service = self.service, options = self.options)
            self.aberto = True
        except WebDriverException:
            print('Verifique sua internet!')
            # 2 segundos para a mensagem poder ser lida
            time.sleep(2)
        except NoSuchDriverException:
            print('Verifique se o ChromeDriver está na sua pasta de instalação!')
            time.sleep(2)
        except SessionNotCreatedException:
            print('Seu webdriver parece ser de uma versão antiga!')
            time.sleep(2)
        except Exception as e:
            print(e)
            time.sleep(2)
            

    def check_receita(self) -> str:
        '''Verifica pelo selenium headless qual a data da última atualização da base da receita federal
        Procura no XPATH da descrição da base de dados a data
        Verifica por data no formato : 'dd/mm/aaaa'

        returns
        -------
        data : str
            Data da última atualização dos dados públicos de CNPJ'''
        
        self.navegador.get('https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj')
        self.navegador.implicitly_wait(10)
        texto = self.navegador.find_element(By.XPATH, '/html/body/div/section/div/div[3]/div[1]/div/div[2]/span').text
        data = list()
        
        lista = list(texto)
        i = 0
        numeros = {'0','1','2','3','4','5','6','7','8','9'}
        # Itera por cada item do texto procurando por um valor numérico
        while i < len(lista):
            if lista[i] in numeros:
                try:
                    # Checa se os próximos caracteres são números e backslashes, para entender se o texto configura uma data
                    next_num = (lista[i+1], lista[i+3], lista[i+4], lista[i+6], lista[i+7], lista[i+8], lista[i+9])
                    next_num_bool = True
                    next_backslash = (lista[i+2], lista[i+5])
                    next_backs_bool = True
                    for _ in next_num:
                        if _ not in numeros:
                            next_num_bool = False
                    for _ in next_backslash:
                        if _ != '/':
                            next_backs_bool = False
                    # Se os próximos caracteres configuram uma data, adiciona o resultado à lista
                    if next_num_bool and next_backs_bool:
                        z = 1
                        res = lista[i]
                        for e in range(9):
                            res += str(lista[i+z])
                            z += 1
                        data.append(res)
                except IndexError:
                    pass #pass ou i +=1?
            i += 1
        return data[-1]
        

    def check_abecs(self) -> list:
        '''Verifica pelo selenium headless qual a data da última atualização do de:para da ABECS e lista de CNPJs

        returns
        -------
        file : list
            Nomes dos últimos files do de_para e lista de cnpjs determinados disponibilizado pela ABECS'''
    
        self.navegador.get('https://www.abecs.org.br/consulta-mcc-individual')
        self.navegador.implicitly_wait(10)
        depara = self.navegador.find_element(By.XPATH, '/html/body/div/main/section/div/div/div[1]/div[2]/a').get_attribute('href')
        cnpj = self.navegador.find_element(By.XPATH, '/html/body/div/main/section/div/div/div[2]/div[2]/p[1]/a').get_attribute('href') 

        return [depara, cnpj]


def start_update(dados):
    dados.permite_update()
    dados.read_version()


def web_scrape(browser, dados) -> list:
    '''Para cada item a ser buscado na web, faz um try except.
    Cria duas listas, uma de valroes que sofreram updates.
    Outra lista é de erros'''

    print('''Buscando atualizações das bases de dados!
--------------------------------------------------------------------------------''')
    
    res_scraping = list()
    erros_scraping = list()

    # Se o webdriver abriu, tenta fazer webscraping dos dados da receita e ABECS
    # Se conseguir, adiciona o item à lista de respostas
    # Se não conseguir, adiciona a lista de erros
    
    # Se houver alguma erros de XPATH, corrigir nas funções:
    # check_receita() e check_abecs()
    try:
        dados.current_versions['receita'].append(browser.check_receita())
        res_scraping.append('receita')
        print(f'Busca // Dados da Receita: {dados.current_versions["receita"][1]}')
    except NoSuchElementException: # erros de elemento XPATH
        print('Não foi possível encontrar o XPATH da (receita) : Corrigir no script - Avise o responsável pela automação')
        erros_scraping.append('receita')
    except WebDriverException: # erros de internet
        print('Não foi possível conectar ao site da Receita: Verifique sua internet')
        erros_scraping.append('receita')
    except Exception as e: # outros erros não mapeados
        print(e)
        erros_scraping.append('receita')
            
    try:
        dados.current_versions['depara_abecs'].append(browser.check_abecs()[0])
        res_scraping.append('depara_abecs')
        print(f'Busca // De para ABECS: {dados.current_versions["depara_abecs"][1].split("/")[-1]}')
    except NoSuchElementException:
        print('Não foi possível encontrar o XPATH do (de_para) : Corrigir no script - Avise o responsável pela automação')
        erros_scraping.append('depara_abecs')        
    except WebDriverException:
        print('Não foi possível conectar ao site da ABECS (de_para): Verifique sua internet')
        erros_scraping.append('depara_abecs')
    except Exception as e:
        print(e)
        erros_scraping.append('depara_abecs')
            
    try:
        dados.current_versions['lista_cnpj'].append(browser.check_abecs()[1])
        res_scraping.append('lista_cnpj')
        print(f'Busca // Lista CNPJs ABECS: {dados.current_versions["lista_cnpj"][1].split("/")[-1]}')
    except NoSuchElementException:
        print('Não foi possível encontrar o XPATH da (lista_cnpj) : Corrigir no script - Avise o responsável pela automação')
        erros_scraping.append('lista_cnpj')
    except WebDriverException:
        print('Não foi possível conectar ao site da ABECS (lista_cnpj): Verifique sua internet')
        erros_scraping.append('lista_cnpj')
    except Exception as e:
        print(e)
        erros_scraping.append('lista_cnpj')

    if erros_scraping:
        for e in erros_scraping:
            print(f'Erro no webscraping : {e}')
        time.sleep(2)
              
    browser.navegador.quit()

    return (res_scraping, erros_scraping)    
        

def end_update(dados, res_scraping : list):
    '''Se condições para update forem verdadeiras, chama as funções de update'''

    # Pega os dados do scraping, e atualiza se:
    # 1) O webscraping retornou uma informação válida, oriunda da lista de web_scrape()
    # 2) O valor do scraping é diferente da base vigente
    # 3) Se a opção de update está habilitada
    
    if res_scraping:
        if 'receita' in res_scraping and (dados.current_versions['receita'][0] != dados.current_versions['receita'][1]) and dados.update:
            dados.update_download_receita()
            dados.atualizar_receita()

        if 'depara_abecs' in res_scraping and (dados.current_versions['depara_abecs'][0] != dados.current_versions['depara_abecs'][1]) and dados.update:
            dados.update_depara_abecs()

        if 'lista_cnpj' in res_scraping and (dados.current_versions['lista_cnpj'][0] != dados.current_versions['lista_cnpj'][1]) and dados.update:
            dados.update_cnpjs_abecs()

    # Se a opção de update estiver desabilitada no config.txt, dá um aviso
    if not dados.update:
        print('''Update está desabilitado!''')
        time.sleep(1)
    else:
        print('''Updates habilitados''')
        time.sleep(1)


if __name__ == "__main__":
    # Inicializa classes
    browser = Browser()
    dados = Dados()
    interface = GUI()
    # Cria database se ela não existir
    dados.cria_database()
    # Verifica o arquivo 'config.txt'
    start_update(dados)
    # Verifica os dados na web e atualiza o .db se necessário
    browser.abrir_navegador()
    if browser.aberto:
        end_update(dados, web_scrape(browser,dados)[0])
    # Inicializa a GUI de busca de CNPJ
    interface.main_loop()
    
quit()
