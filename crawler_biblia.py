'''

Criado em 28.04.2018 para fazer o crawler de várias versoes da biblia.

Fluxo:
01. Carrega os dados da biblia.
02. Capturar o capitulo.
'''


from bs4 import BeautifulSoup, NavigableString, Tag
import requests
import re
import pandas as pd
import numpy as np
import time

re_letra = re.compile('\W')

arquivo_biblia = 'C:\\Work\\csv\\biblia_pontuada.csv'

def get_sopa(url):
    req = requests.get(url)
    data = req.text
    soup = BeautifulSoup(data, "html5lib")
    return soup

#https://www.bibliaonline.com.br/acf/gn/1
def get_texto_cap(url_cap):
    global re_letra
    sopa = get_sopa(url_cap)
    div = sopa.find_all('div',{'class':['passage']})[0]
    return [  {
                    'versao' : url_cap.split('/')[-3],
                    'sg_livro' : url_cap.split('/')[-2],
                    'capitulo' :url_cap.split('/')[-1],
                    'versiculo' : p.find_all('sup')[0].contents[0],
                    'texto': p.contents[1].strip()
                } for p in div.find_all('p')]

def monta_biblia_versao(url_v):
    texto = []
    df_d = pd.read_csv('C:\\Work\\csv\\dados_biblia.csv',encoding='latin-1',sep=';')
    #ordem	livro	sigla	qtd_cap	qtd_versos	testamento
    for t in df_d.itertuples(index=False,name=None):
        print('Capturando %s' %t[1])
        for c in range(1,t[3]+1):
            #print('Capitulo %i' %c, end='\r')
            texto.extend(get_texto_cap(url_v+'/%s/%i' %(t[2].lower(),c)))
        try:
            pd.read_csv(arquivo_biblia).append(pd.DataFrame(texto)).to_csv(arquivo_biblia,index=False)
            texto = []
        except:
            pd.DataFrame(texto).to_csv(arquivo_biblia,index=False)
            texto = []
            pass


if __name__ == '__main__':
    localtime = time.asctime( time.localtime(time.time()) )
    print('Inicio do processamento ', localtime)
    versoes = ['acf','nvi']
    for v in versoes:
        monta_biblia_versao('https://www.bibliaonline.com.br/'+ v)
