import pandas as pd
import sqlite3
import PySimpleGUI as sg
import wget
import os
import shutil
from zipfile import ZipFile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import time

# by Lucas Staub
# finalizado em xx/xx/xxxx

# Lógica do programa:
# Busca nas fontes de dados por webscraping (dados abertos da receita federal e site da ABECS) os arquivos para criar uma database, que servirá como fonte de consulta por SQL
# Faz verificação a cada abertura do programa, comparando as versões dos dados da receita e abecs com os vigentes no database /
# Caso haja novos dados, formata baixa estes dados e atualiza o DB local
# Utiliza uma GUI para consultas de SQL do database local

# Limitações:
# É necessário um webdriver atualizado para fazer o webscraping, ou o programa não funciona
# Pode haver defasagem de dados, uma vez que:
    # Base da receita federal é atualizada uma vez por mês
    # Se o CNPJ não é encontrado, é feito um webscraping que quebra CAPTCHAs, mas este pode não funcionar, retornando o CNPJ sem informações


# Quantidade de dados gera um arquivo .db de aproximadamente 4gb

class Dados():
    '''Classe para buscar, atualizar e tratar dados das diferentes fontes'''
    def __init__(self):
        '''Cria variáveis de versionamentos dos dados'''

        ''' Versões vigentes dos arquivos são armazenados em um dict, lidos do 'config.txt'
        Valores deste dict é uma tupla, com versão lida do file inicial, e versão atual lida pelo webscraping
        Se valores da tupla forem diferentes, atualiza o database e sobrescreve a nova versão no .txt ao finalizar o update
        Keys são sempre: {'receita', 'abecs', 'lista_cnpj'}'''
        self.current_versions = dict()
        self.current_versions['receita'] = []
        self.current_versions['abecs'] = []
        self.current_versions['lista_cnpj'] = []
        self.update = True
        self.path_script = os.path.abspath(os.path.dirname(__file__))
        self.path_temp = self.path_script + '\\temp'
    

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
                file.write('abecs=nula\n')
                file.write('lista_cnpj=nula\n')
                file.write('permite_update=Sim')
                file.close()


    def cria_database(self):
        '''Inicializa database e table se elas ainda não existirem'''
        
        connection = sqlite3.Connection(os.path.join(self.path_script,'database.db'))
        cursor = connection.cursor()
        
        # Receita Federal
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS DADOS_RECEITA
        ([CNPJ_RECEITA] TEXT PRIMARY KEY, [SITUACAO_CADASTRAL_RECEITA] TEXT, [CNAE_PRINCIPAL_RECEITA] TEXT, [CNAES_SECUNDARIOS_RECEITA] TEXT)
        ''')
        
        # Lista de MCCs determinados pelas bandeiras
        cursor.execute( '''
        CREATE TABLE IF NOT EXISTS MCCS_DETERMINADOS
        ([CNPJ_BANDEIRA_ABECS] TEXT PRIMARY KEY, [MCC_BANDEIRA_ABECS] TEXT, [TIPO_ABECS] TEXT, [DATA_DETERMINACAO_ABECS] TEXT)''')
        
        # Relação de CNAE/MCC da ABECS
        cursor.execute( '''
        CREATE TABLE IF NOT EXISTS DEPARA
        ([CNAE_PRINCIPAL_ABECS] TEXT PRIMARY KEY, [MCC_PRINCIPAL_ABECS] TEXT, [MCCS_SECUNDARIOS_ABECS] TEXT)
        ''')
        
        connection.commit()


    def testa_database(self):
        pass
        '''Roda um SQL statement para cada tabela para verificar se ela está populada.
        Se não estiver sobrescreve o config.txt para indicar o item como nula'''

        #connection = sqlite3.Connection(os.path.join(self.path_script,'database.db'))
        #cursor = connection.cursor()
        #cursor.execute('''SELECT COUNT(*) FROM''')


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
                file.write('abecs=nula\n')
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

            print(f'''
            --------------------------------------------------------------------------
            Arquivos da receita devem ser atualizados!
            A versão atual da base é {self.current_versions["receita"][0]},
            Há uma nova versão: {self.current_versions["receita"][1]}
            --------------------------------------------------------------------------
            Se você não quiser atualizar a base, leia o procedimento no Confluence
            --------------------------------------------------------------------------
            Ou vá até a pasta da sua instalação e:
            Abra a pasta 'dist' e procure pelo arquivo 'config.txt'
            Substitua permite_update=sim
            por
            permite_update=nao
            Abre novamente o programa e ele não irá atualizar
            --------------------------------------------------------------------------
            Para reativar as atualizações
            no 'config.txt'
            deixe como:
            permite_update=sim
            --------------------------------------------------------------------------x
            ''')
            
            # Usa replace pq não pode '/' em nome de arquivos!
            data_arquivos_receita = self.current_versions["receita"][1].replace("/","_")
            faltam_arquivos = True
            i = 0
            while faltam_arquivos == True:
                # Se o arquivo Estabelecimento(n)_data_arquivo existe, itera para o próximo download
                if os.path.isfile(os.path.join(self.path_script,f'temp\\Estabelecimentos{i}_{data_arquivos_receita}.zip')) \
                or os.path.isfile(os.path.join(self.path_script,f'temp\\Estabelecimentos{i}_{data_arquivos_receita}.ESTABELE')):
                    i += 1
                    print(f'Já existe o {i}º arquivo da receita')
                else:
                    # Se ele não existe, tenta baixar até acabar o total de arquivos disponíveis na receita federal
                    try:
                        print(f'Baixando o {(i+1)}º arquivo da receita')
                        wget.download(url=f'{files}{i}.zip', out=os.path.join(self.path_script,f'temp\\Estabelecimentos{i}_{data_arquivos_receita}.zip'))
                        i += 1
                    except Exception as e:
                    # Se não conseguir baixar ou não houver arquivos para baixar, ou outros erros, fecha o loop
                        print(f'Arquivo {i} = {e}')
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
                        os.remove(os.path.join(self.path_script,f'temp\\{file}'))



    def atualizar_receita(self):
        '''Para cada um dos arquivos .ESTABELE na pasta \temp, pega os campos relevantes:
        CNPJ, CNAE_PRIMÁRIO, CNAE SECUNDÁRIO E SITUAÇÃO NA RECEITA
        Insere no database, dando drop no anterior'''

        # Conecta ao database
        connection = sqlite3.Connection(os.path.join(self.path_script,'database.db'))
        cursor = connection.cursor()

        # Dropa a table anterior de dados da receita e recria ela
        # Evita erros de registro
        cursor.execute('''DROP TABLE DADOS_RECEITA''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS DADOS_RECEITA
        ([CNPJ_RECEITA] TEXT PRIMARY KEY, [SITUACAO_CADASTRAL_RECEITA] TEXT, [CNAE_PRINCIPAL_RECEITA] TEXT, [CNAES_SECUNDARIOS_RECEITA] TEXT)
        ''')

        # Lê lista de arquivos .ESTABELE 
        estabele = list()
        for file in os.listdir(os.path.join(self.path_script,'temp\\')):
            if file.endswith('.ESTABELE'):
                estabele.append(os.path.join(self.path_script,f'temp\\{file}'))
                
        # Retira os dados desnecessários dos arquivos e insere na table
        for e in estabele:
            print(f'Inserindo o arquivo {e} na base de dados')
            with open(e, mode='r', encoding='latin-1') as arq:
                for z in arq.readlines():
                    x = z.split(";")
                    cnpj = (x[0] + x[1] + x[2]).replace('"','')
                    situacao_cadastral = x[5].replace('"','')
                    cnae_p = x[11].replace('"','')
                    cnae_s = x[12].replace('"','')
                    dados = (cnpj, situacao_cadastral, cnae_p, cnae_s)
                    cursor.execute('''INSERT OR REPLACE INTO DADOS_RECEITA VALUES (?,?,?,?)''', dados)

        '''Dá commit() no SQL e testa se a database está funcionando com o CNPJ da Gétinéti
        Se estiver funcionando, apaga todos os arquivos baixados anteriormente
        Atualiza config '''

        connection.commit()
        z = cursor.execute('''SELECT * FROM DADOS_RECEITA WHERE CNPJ_RECEITA = "10440482000154"''').fetchone()
        if z:
            # Deleta pasta \temp
            shutil.rmtree(os.path.join(self.path_script,'temp\\'))
            with open('config.txt', mode='r') as read:
                atual = read.readlines()
                nova_data_update = f'receita={self.current_versions["receita"][1]}\n'
                atual[0] = nova_data_update
                read.close()
                with open('config.txt', mode='w') as write:
                    write.writelines(atual)
            print('Base da Receita Federal atualizada!')
        # Se a query com o CNPJ da Get falhar, não excluí os arquivos da pasta \temp, para rodar o processo outra vez           
        else:
            print('erro ao subir os arquivos do database')




    def update_cnpjs_abecs(self):
        '''Se a configuração permitir updates, atualiza a relação de CNPJs determinados por bandeira da ABECS
        Verifica se o arquivo .xlsx já existe, se não baixa por wget e sobe no banco de dados
        Ao fim do processo exclui o excel e atualiza o .config'''
        
        if self.update and (self.current_versions['lista_cnpj'][0] != self.current_versions['lista_cnpj'][1]):
            # Link é obtido pelo selenium, pelo href
            # Nome do excel é a última parte do link
            nome_excel = self.current_versions['lista_cnpj'][1].split('/')[-1]
            link = self.current_versions['lista_cnpj'][1]
            
            print(f'''Arquivos de CNPJ Determinados da ABECS receita deve ser atualizado!
            A versão atual da base é {self.current_versions["lista_cnpj"][0]},
            Há uma nova versão: {self.current_versions["lista_cnpj"][1]}''')
            
            # Se o arquivo ainda não foi baixado, faz download dele
            if not os.path.isfile(os.path.join(self.path_script, nome_excel)):
                wget.download(link)

            file = os.path.join(self.path_script, nome_excel)
            
            # Cria dataframe do excel e formata dados
            df = pd.read_excel(file, names =['CNPJ','MCC','TIPO','DATA'], dtype={'CNPJ' : 'string', 'MCC' : 'string', 'TIPO' : 'string', 'DATA' : 'string'})
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
            # Cnpj é a key do dicionário, e outros dados estão em uma tupla nos values deste dict, desempacota todas as infos necessária e sobe elas para o .db por meio de uma tupl

            connection = sqlite3.Connection('database.db')
            cursor = connection.cursor()

            cursor.execute('''DROP TABLE MCCS_DETERMINADOS''')
        
            cursor.execute( '''
            CREATE TABLE IF NOT EXISTS MCCS_DETERMINADOS
            ([CNPJ_BANDEIRA_ABECS] TEXT PRIMARY KEY, [MCC_BANDEIRA_ABECS] TEXT, [TIPO_ABECS] TEXT, [DATA_DETERMINACAO_ABECS] TEXT)
            ''')
    
            
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

            # Terminado a inserção, exclui o excel e atualiza o file .config
            os.remove(file)
            with open('config.txt', mode='r') as read:
                atual = read.readlines()
                print(atual)
                novo_link_update = f'lista_cnpj={self.current_versions["lista_cnpj"][1]}\n'
                atual[2] = novo_link_update
                print(atual[2])
                read.close()
                with open('config.txt', mode='w') as write:
                    write.writelines(atual)
            print('CNPJs Determinados atualizados!')

            

    def update_depara_abecs(self):
        '''Se a configuração permitir updates, atualiza a relação de:para da ABECS
        Verifica se o arquivo .xlsx já existe, se não baixa por wget e sobe no banco de dados
        Ao fim do processo exclui o excel e atualiza o .config'''

        if self.update and (self.current_versions['abecs'][0] != self.current_versions['abecs'][1]):
            # Link é obtido pelo selenium, pelo href
            # Nome do excel é a última parte do link
            nome_excel = self.current_versions['abecs'][1].split('/')[-1]
            link = self.current_versions['abecs'][1]
            
            print(f'''Arquivos de DE:PARA CNAE da ABECS receita deve ser atualizado!
            A versão atual da base é {self.current_versions["abecs"][0]},
            Há uma nova versão: {self.current_versions["abecs"][1]}''')
            
            # Se o arquivo ainda não foi baixado, faz download dele
            if not os.path.isfile(os.path.join(self.path_script, nome_excel)):
                wget.download(link)

            file = os.path.join(self.path_script, nome_excel)

            # Cria o dataframe do excel de de_para da ABECS

            df = pd.read_excel(file, header=1, usecols=[3,5,7,9,11,13,15,17,19],\
                               names=['CNAE','MCCP','MCC1','MCC2','MCC3','MCC4','MCC5','MCC6','MCC7'])
            df = df.fillna(0)
            lista = df.values.tolist()

            # Itera pela lista, e insere os dados em uma table do database, dropando os valores anteriores
            connection = sqlite3.Connection('database.db')
            cursor = connection.cursor()

            cursor.execute('''DROP TABLE DEPARA''')

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

            # Terminado a inserção, exclui o excel e atualiza o file .config
            os.remove(file)
            with open('config.txt', mode='r') as read:
                atual = read.readlines()
                novo_link_update = f'abecs={self.current_versions["abecs"][1]}\n'
                atual[1] = novo_link_update
                read.close()
                with open('config.txt', mode='w') as write:
                    write.writelines(atual)
            print('De para atualizado!')
        



class GUI():
    '''Interface gráfica'''

    def __init__(self):
        self.layout_main_gui = []
        self.update_gui = []



class Scraper():
    '''Webdriver'''
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.service = webdriver.ChromeService()
        self.open = False
        self.driver_atualizado = False
        self.driver_instalado = False


             
    def add_options(self):
        '''Adiciona os argumentos de opções do browser'''
        self.options.add_argument('--headless')



    def versao_driver(self):
        pass



    def abrir_navegador(self):
        '''Abre o navegador.
        Se o webdriver não estiver na pasta ou estiver desatualizado, baixa a versão atual'''
        try:
            self.navegador = webdriver.Chrome(service = self.service, options = self.options)
        except:
            pass

            

    def check_receita(self) -> str: # deveria ter try e except?? se der erro no check dwonload_receita nem deveria rodar
        # dá pra fazer if check_receita then atualiza else print('erro scraping')?
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
                    pass #pass ou i +=1 ?
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



edge = Scraper()
dados = Dados()
interface = GUI()
dados.cria_database()
dados.permite_update()
dados.read_version()
edge.add_options()
edge.abrir_navegador()
dados.current_versions['receita'].append(edge.check_receita())
dados.current_versions['abecs'].append(edge.check_abecs()[0])
dados.current_versions['lista_cnpj'].append(edge.check_abecs()[1])
edge.navegador.quit()
# se der erro ao abrir o navegador, não usar funções de atualização!
dados.update_download_receita()
# criar ifs, se tiver atualizado não precisa chamar o dados.atualizar!
dados.atualizar_receita()
dados.update_cnpjs_abecs()
dados.update_depara_abecs()
# criar funções da GUI e loop:
# consulta individual
# consulta por raiz
# consulta em lote
# extração em lote
