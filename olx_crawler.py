from bs4 import BeautifulSoup, NavigableString, Tag
import requests
import re
import pandas as pd
import numpy as np
import time
from threading import Thread
import _thread
import platform
localtime = time.asctime( time.localtime(time.time()) )
print('Inicio do processamento ', localtime)

QUANTIDADE_DE_THREADS = input('Qtd threads: ')
while  not QUANTIDADE_DE_THREADS.isdigit():
    print('Enquanto nao digitar somente numeros eu irei te perguntar.')
    QUANTIDADE_DE_THREADS = input('Qtd threads: ')

QUANTIDADE_DE_THREADS = int(QUANTIDADE_DE_THREADS)
qtd_reg = [0] * QUANTIDADE_DE_THREADS
hora_inicial = [0] * QUANTIDADE_DE_THREADS
ttl_reg_th = [0] * QUANTIDADE_DE_THREADS

def formataFonePsqOLX(strFone):
    ddd = strFone[:2]
    nonoDigito = strFone[2:3]
    primeiros4 = strFone[3:7]
    ultimos4 = strFone[7:11]
    return '(' + ddd + ')+' + nonoDigito + '+' + primeiros4 + '-' + ultimos4

def get_qtd_por_ctgr(sopa_pg_olx):
    qtd_ctgr = []
    for divtag in sopa_pg_olx.find_all('div', {'class': 'search-subcategory-nav'}):
        for span_tag in divtag.find_all('span', {'class': 'qtd'}):
            qtd = re.sub('[^0-9]','', span_tag.text)
            if qtd == '':
                qtd_ctgr.append(0)
            else:
                qtd_ctgr.append(int(qtd))
    return qtd_ctgr

def get_qtd_por_uf(sopa_pg_olx):
    qtd_uf = []
    for divtag in sopa_pg_olx.find_all('div', {'class': 'linkshelf-tabs-content country'}):
        for span_tag in divtag.find_all('span', {'class': 'qtd'}):
            qtd = re.sub('[^0-9]','', span_tag.text)
            if qtd != '' and qtd != '0':
                qtd_uf.append(int(qtd))
    return qtd_uf

def captura_metadados_anunciante(linha):
    global QUANTIDADE_DE_THREADS,qtd_reg,hora_inicial,ttl_reg_th
    n_th = linha['CD_CLI'] % QUANTIDADE_DE_THREADS
    qtd_reg[n_th] += 1
    strFone = str(linha['NR_DDD']) + str(linha['NR_TEL'])
    c = []
    u = []
    if len(strFone) == 11:
        url = 'http://www.olx.com.br/brasil?q=' + formataFonePsqOLX(strFone)
        req = requests.get(url)
        data = req.text
        soup = BeautifulSoup(data, "html5lib")
        c = get_qtd_por_ctgr(soup)
        u = get_qtd_por_uf(soup)
    if qtd_reg[n_th] % 100 == 0:
        estimativa_tempo =  ((time.time()-hora_inicial[n_th])/qtd_reg[n_th]) * (ttl_reg_th[n_th] - qtd_reg[n_th])
        print('%i registros pesquisados. \n%i segundos para o termino da thread %i.' %(qtd_reg[n_th], estimativa_tempo, n_th))
    return {
        'categorias' : c,
        'uf' : u
    }

def ajusteParaThread(df, numeroDaThread):
    global hora_inicial, ttl_reg_th
    if df is None:
        print('DataFrame None. Thread %i' %numeroDaThread)
    elif len(df.index) == 0:
        print('DataFrame vazio. Thread %i' %numeroDaThread)
    else:
        ttl_reg_th[numeroDaThread] = len(df.index)
        print('%i registros para pesquisa na thread %i.' %(ttl_reg_th[numeroDaThread],numeroDaThread))
        hora_inicial[numeroDaThread] = time.time()
        print('Pesquisando na OLX thread %i.' %(numeroDaThread))
        df['psq_olx'] = df.apply(captura_metadados_anunciante,axis=1)
        nm_arq = ('rstd_psq_%s.csv' %str(numeroDaThread))
        if platform.system()[:7].lower() == 'windows':
            nm_arq = ('C:\\BB\\csv\\rstd_psq_%s.csv' %str(numeroDaThread))
        df.to_csv(nm_arq)
        localtime = time.asctime( time.localtime(time.time()) )
        print('Termino do processamento da thread %i - %s' %(numeroDaThread,localtime))


if __name__ == '__main__':
    localtime = time.asctime( time.localtime(time.time()) )
    nm_arq = '/home/perfil/csv/TELS.csv'
    if platform.system()[:7].lower() == 'windows':
        nm_arq = 'C:\\BB\\csv\\TELS.csv'

    print('Carregando arquivo %s - %s.' %(nm_arq,localtime))

    df = pd.read_csv(nm_arq)
    ttl_reg = len(df.index)
    print('%i registros para pesquisa' %ttl_reg)

    localtime = time.asctime( time.localtime(time.time()) )
    print('Pesquisando na OLX ', localtime)
    #print(df.head())

    threads = []
    for i in range(0,QUANTIDADE_DE_THREADS):
        t = Thread(target=ajusteParaThread, args=[df.loc[df['CD_CLI'] % QUANTIDADE_DE_THREADS == i],i])
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    localtime = time.asctime( time.localtime(time.time()) )
    print('Termino do processamento', localtime)
