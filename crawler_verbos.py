'''

Criado em 25.04.2018 para criar um arquivo com conjugacoes dos verbos da lingua portuguesa.

Fluxo:
01. Pegar a lista dos verbos regulares.
02. Capturar a conjucao.
03. Fazer o mesmo com os verbos irregulares.

'''


from bs4 import BeautifulSoup, NavigableString, Tag
import requests
import re
import pandas as pd
import numpy as np
import time
from threading import Thread
import _thread
import platform
from itertools import chain
def get_sopa(url):
    req = requests.get(url)
    data = req.text
    soup = BeautifulSoup(data, "html5lib")
    return soup

def get_verbos(url):
    sopa = get_sopa(url)
    lista_verbos = []
    for divtag in sopa.find_all('div', {'class': 'wrapper'}):
        for a in divtag.find_all('a'):
            if 'verbo' in a['href']:
                lista_verbos.append(a['href'])
    return lista_verbos

def get_dados_verbo(url,i):
    print('%i. Pesquisados' %i, end='\r')
    sopa = get_sopa(url)
    dados = []
    try:
        verbo = sopa.find('h1',{'class': 'nmt'}).text.lower().replace('verbo','').strip()
        div_info = sopa.find_all('div',{'class': 'info-v'})[0]
        infos_procuradas = ['gerúndio','particípio passado','infinitivo']
        for st in div_info.find_all('strong'):
            if st.contents[0].strip().lower() in infos_procuradas:
                sp_info = st.findNext('span').find_all('span',{'class' : 'f'})[0]
                if sp_info.contents[0]:
                    dados.append(
                    {
                    'verbo' : verbo,
                    #'modo' : modo,
                    'tempo' : st.contents[0].strip().lower(),
                    'pessoa' : '',
                    'palavra' : sp_info.contents[0].strip().lower()
                    })
        pessoas = ['eu','tu','ele','nós','vós','eles']
        for divtag in sopa.find_all('div', {'class': 'tempos'}):
            for divc in divtag.find_all('div', {'class': 'tempo-conjugacao'}):
                tempo = divc.find('h4').contents[0].strip().lower()
                for p in pessoas:
                    try:
                        tempos_dif = ['imperativo afirmativo', 'imperativo negativo', 'infinitivo pessoal']
                        sp_pss = divc.find(text=re.compile(p))
                        sp_palavra = sp_pss.findNext('span') if tempo not in tempos_dif else sp_pss.findPrevious('span').findPrevious('span')
                        palavra = sp_palavra.contents[0].strip().lower() if tempo not in tempos_dif else sp_palavra.contents[0].strip().lower()
                        if palavra != 'não o' and palavra != 'por o':
                            dados.append(
                            {
                            'verbo' : verbo,
                            #'modo' : modo,
                            'tempo' : tempo,
                            'pessoa' : p,
                            'palavra' : palavra
                            })
                    except Exception as e:
                        pass
    except Exception as e:
        pass
    return dados

if __name__ == '__main__':
    localtime = time.asctime( time.localtime(time.time()) )
    print('Inicio do processamento ', localtime)
    url_base = 'https://www.conjugacao.com.br'
    lista_verbos = []
    try:
        lista_verbos = pd.read_csv('verbos_psq.csv')['verbo'].values
    except Exception as e:
        lista_verbos = list(chain(*[get_verbos(url_base+'/verbos-populares/' + str(i)) for i in range(1,202)]))
        df_v = pd.DataFrame(lista_verbos,columns=['verbo'])
        df_v.to_csv('verbos_psq.csv',index=False)
        pass
    print('%i verbos para capturar' %len(lista_verbos))
    print('Inicio da captura das conjugacoes %s' %time.asctime( time.localtime(time.time()) ))
    conjugacoes = []
    try:
        verbos = pd.read_csv('C:\\Work\\csv\\verbos.csv')[['palavra','pessoa','tempo','verbo']]
        verbos = verbos[(verbos.verbo != 'por o') & (verbos.verbo != 'não o') & (verbos.palavra.notin(['eu','tu','ele','nós','vós','eles']))]
        conjugacoes = list(verbos.T.to_dict().values())
        for v in verbos.verbo.unique():
            lista_verbos.remove(v)
    except Exception as e:
        pass
    print('%i verbos já capturados.' %len(verbos.verbo.unique()))
    for i,v  in enumerate(lista_verbos):
        conjugacoes.extend(get_dados_verbo(url_base + v,i))
        if i%100 == 1:
            pd.DataFrame(conjugacoes).to_csv('C:\\Work\\csv\\verbos.csv',index=False)
    print('%i conjugacoes' %len(conjugacoes))
    pd.DataFrame(conjugacoes).to_csv('C:\\Work\\csv\\verbos.csv',index=False)
    #pd.DataFrame(conjugacoes).to_csv('verbos.csv')
    localtime = time.asctime( time.localtime(time.time()) )
    print('Término do processamento ', localtime)
