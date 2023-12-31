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
            # Arquivo não existe, cria ele
            with open('config.txt', mode='w') as file:
                file.write('receita=\n')
                file.write('abecs=\n')
                file.write('lista_cnpj=\n')
                file.write('permite_update=Sim')
                file.close()


    def permite_update(self):
        '''Configuração vinculada ao self.update, que determina se a database pode ser atualizada ou não.
        Variável nasce como False, e se estiver como 'Sim' no arquivo de configurações, vai fazer scraping e updates.
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
                file.write('receita=\n')
                file.write('abecs=\n')
                file.write('lista_cnpj=\n')
                file.write('permite_update=sim')
                file.close()

        

    def update_download_receita(self):
        '''Verifica se todos os arquivos .zip já foram baixados da receita.
        Enquanto houverem arquivos Estabelecimentos(n).zip para baixar, continua tentando pelo wget.
        Depois de todos os files baixados, descompacta todos eles
        Insere os files descompactados no database
        Após o êxito do commit() no SQL, deleta a pasta temporária e todos os downloads
        ''' 

        '''Se a configuração permitir updates, e a versão vigente dos dados for diferente da encontrada no webscraping,
        cria uma pasta temporária para os downloads dos novos arquivos'''
        
        if self.update and (self.current_versions['receita'][0] != self.current_versions['receita'][1]):
            '''Cria pasta de arquivos temporários se ela não existir'''
            try:
                os.mkdir(self.path_temp)
            except FileExistsError:
                pass

            '''Deleta arquivos .tmp de jobs de download que falharam'''
            for file in os.listdir(os.path.join(self.path_script,'temp\\')):
                if file.endswith(".tmp"):
                    os.remove(os.path.join(self.path_script,f'temp\\{file}'))
            
            files = 'https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos'

            '''Verifica se todos os .zip já foram baixados, se não, baixa eles'''
            data_arquivos_receita = self.current_versions["receita"][1].replace("/","_") # Usar replace pq não pode '/' em nome de arquivos!
            faltam_arquivos = True
            i = 0
            while faltam_arquivos == True:
                # Se o arquivo Estabelecimento(n)_data_arquivo existe, itera para o próximo download
                if os.path.isfile(os.path.join(self.path_script,f'temp\\Estabelecimentos{i}_{data_arquivos_receita}.zip')):
                    i += 1
                    print(f'Já existe o file {i}')
                else:
                    # Se ele não existe, tenta baixar até acabar o total de arquivos disponíveis na receita federal
                    try:
                        print(f'Baixando o {(i+1)}º arquivo da receita')
                        wget.download(url=f'{files}{i}.zip', out=os.path.join(self.path_script,f'temp\\Estabelecimentos{i}_{data_arquivos_receita}.zip'))
                        i += 1
                    except Exception as e:
                    # Se não conseguir baixar ou não houver arquivos para baixar, fecha o loop
                        print(e)
                        faltam_arquivos = False

            #melhorar esta parte para reconhecer se já existe o file não zipado!!!
            #elif file.endswith(".zip"):


            '''Para cada um dos arquivos .ESTABELE pega os campos relevantes e insere no database, dando drop no anterior
            Database é criada na mesma pasta do script'''

            # Cria a db no path do .py
            connection = sqlite3.Connection(os.path.join(self.path_script,'database.db'))
            cursor = connection.cursor()

            # Cria a table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS DADOS_RECEITA
            ([CNPJ_RECEITA] TEXT PRIMARY KEY, [SITUACAO_CADASTRAL_RECEITA] TEXT, [CNAE_PRINCIPAL_RECEITA] TEXT, [CNAES_SECUNDARIOS_RECEITA] TEXT)
            ''')

            # Cria lista de arquivos .ESTABELE
            estabele = list()
            for file in os.listdir(os.path.join(self.path_script,'temp\\')):
                if file.endswith('.ESTABELE'):
                    estabele.append(os.path.join(self.path_script,f'temp\\{file}'))
            # Retira os dados desnecessários e insere na table
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

            # Dá commit() no SQL
            connection.commit()
                        
            '''Após o commit(), deleta a pasta temporária e atualiza config file'''

            shutil.rmtree(os.path.join(self.path_script,'temp\\'))
            with open('config.txt', mode='r') as read:
                atual = read.readlines()
                novo_texto = f'receita={self.current_version["receita"]}\n'
                atual[0] = novo_texto
                read.close()
                with open('config.txt', mode='w') as write:
                    write.writelines(atual)
        else:
            print('dados da receita ok')

    def update_abecs(self):
        pass

    def update_depara(self):
        pass


class GUI():
    '''Interface gráfica'''

    def __init__(self):
        self.layout_main_gui = []
        self.layout_update_gui = []



class Scraper():
    '''Webdriver = Edge'''
    def __init__(self):
        self.options = webdriver.EdgeOptions()
        self.service = webdriver.EdgeService()
        self.open = False
        self.driver_atualizado = False 

        
        
    def add_options(self):
        '''Adiciona os argumentos de opções do browser'''
        self.options.add_argument('--headless')



    def versao_driver(self):
        pass

    def abrir_navegador(self):
        '''Abre o navegador'''
        self.navegador = webdriver.Edge(service = self.service, options = self.options)
        

    def check_receita(self) -> str:
        '''Verifica pelo selenium headless qual a data da última atualização da base da receita federal

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
                    pass
            i += 1
        return data[-1]
        

    def check_abecs(self):
        '''Verifica pelo selenium headless qual a data da última atualização do de_para da ABECS

        returns
        -------
        file : str
            Nome do último file do de_para disponibilizado pela ABECS'''
    
        self.navegador.get('https://www.abecs.org.br/consulta-mcc-individual')
        self.navegador.implicitly_wait(10)
        depara = self.navegador.find_element(By.XPATH, '/html/body/div/main/section/div/div/div[1]/div[2]/a').get_attribute('href')
        cnpj = self.navegador.find_element(By.XPATH, '/html/body/div/main/section/div/div/div[2]/div[2]/p[1]/a').get_attribute('href') 

        return [depara, cnpj]
    

    def scrape_cnpj(self, cnpj : list):
        pass



edge = Scraper()
dados = Dados()
interface = GUI()
dados.permite_update()
dados.read_version()
edge.add_options()
edge.abrir_navegador()
dados.current_versions['receita'].append(edge.check_receita())
dados.current_versions['abecs'].append(edge.check_abecs()[0])
dados.current_versions['lista_cnpj'].append(edge.check_abecs()[1])
edge.navegador.quit()
dados.update_download_receita()



