# -*- coding: utf-8 -*-

#import libraries
import sys
import glob
from datetime import datetime, timedelta
import httplib2
import pandas as pd

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow

import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors


def objetivo2(elem):
    metrics = 'estimatedMinutesWatched,views,shares,likes,dislikes,subscribersGained,subscribersLost,comments,averageViewDuration,averageViewPercentage'
    dimensions = 'day'
    sort = 'day'
    filters = "video==" + elem

    # Crear reporte segun metricas y dimensiones
    analytics_query_response = youtube_analytics.reports().query(
        # ids="contentOwner==%s" % options.content_owner_id,
        ids='channel==MINE',
        # filters="channel==%s" % options.channel_id,
        metrics=metrics,
        dimensions=dimensions,
        startDate='2016-07-01',
        endDate=str(datetime.now())[:10],
        filters=filters,
        # max_results=50,
        # includeHistoricalChannelData=True,
        sort=sort
    ).execute()
    # extraer el vector de columnas
    cols = [None] * len(analytics_query_response.get("columnHeaders", []))
    i = 0
    for column_header in analytics_query_response.get("columnHeaders", []):
        cols[i] = column_header["name"]
        i += 1
    # extraer la matriz de filas
    Matrix = [[0 for x in range(len(analytics_query_response.get("columnHeaders", [])))] for y in
              range(len(analytics_query_response.get("rows", [])))]
    i, j = 0, 0

    for row in analytics_query_response.get("rows", []):
        for value in row:
            Matrix[i][j] = value
            j += 1
        i += 1
        j = 0
    # Exporting a Dataset to CSV, without index and header
    df = pd.DataFrame.from_records(Matrix, columns=cols)
    df['videoId'] = elem
    df.to_csv('Output/Objetivo2/Objetivo2_report_videoId_' + elem + '.csv', index=False)

    return True


if __name__ == '__main__':
    print('\n'+"Consultar Youtube DATA API - UED")
    # Youtube - Una Empresa Docente - Ingresar credenciales aqui
    ued_user_id = 'RCod39Yh39Slo3sAsh45Pl'
    ued_channel_id = 'UCRCod39Yh39Slo3sAsh45Pl'
    api_key = 'Ingrese_Su_API_Key_Aqui'

    # define API services and scope
    CLIENT_SECRETS_FILE = 'Reference_to_credentials_file.json'
    API_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly",
                  "https://www.googleapis.com/auth/yt-analytics.readonly"]
    API_SERVICE_NAME = "youtubeAnalytics"
    API_VERSION = "v2"

    # Autenticate - Youtube API
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=" ".join(API_SCOPES))

    storage = Storage("%s.dat" % API_SERVICE_NAME)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage, args)

    http = credentials.authorize(httplib2.Http())
    youtube = build('youtube', 'v3', http=http)
    youtube_analytics = build(API_SERVICE_NAME, API_VERSION, http=http)

    print('\n'+"Identificar etiquetas asociadas a los esquemas de difusion")
    # asignar esquema de difusion a cada video
    # left join  videoSummary_UED.csv y Esquemas_UED.csv
    #abrir el archivo con la lista de videos
    FileEsq = 'Output/Esquemas_UED.csv'
    df_esquema = pd.read_csv(FileEsq, sep = '|')
    print(df_esquema.shape)
    df_esquema.head()
    #convertir dataframe en diccionario
    ED = df_esquema.set_index('Etiqueta').T.to_dict('list')
    print(ED)

    #obtener la lista de videos del canal
    #guarda en un csv la lista de videos: identificador, titulo, fecha de publicacion y etiquetas
    print('\n'+"Consultar lista de videos del canal UED, consultar: videoSummary_UED.csv")
    res = youtube.channels().list(
                                  mine = True,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    videos = []
    next_page_token = None
    i=0

    while 1:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                            part='snippet',
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        videos += res['items']
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break


    #exportar el reporte por video
    with open("Output/videoSummary_UED.csv", 'w', encoding = 'utf-8') as output_file:
        output_file.write('videoId|title|publishedAt|esquemaId|esquemaDifusion|tags\n')
        #solo por pruebas**** [:3]
        for video in videos:
            res = youtube.videos().list(id=video['snippet']['resourceId']['videoId'],
                                        part='snippet'
                                        ).execute()
            try:
                etiquetas = res.get('items')
                tags = etiquetas[0]['snippet']['tags']
                for elem in tags:
                    try:
                        info = ED[elem]
                        esqdif = str(info[0])+"|"+str(info[1])
                        break
                    except:
                        esqdif = "99|Ninguna"
                output_file.write(video['snippet']['resourceId']['videoId'] + '|' + video['snippet']['title'] + '|' + video['snippet']['publishedAt'][:16] + '|' + esqdif +'|'+ str(etiquetas[0]['snippet']['tags']) + '\n')
            except:
                print(video['snippet']['resourceId']['videoId']+ " - Sin etiquetas")
                output_file.write(video['snippet']['resourceId']['videoId'] + '|' + video['snippet']['title'] + '|' + video['snippet']['publishedAt'][:16]+'|'+ '99|Ninguna|sin_etiquetas\n')
                i +=1


    output_file.close()


    print("Generar los reportes videoMetrics_UED.csv y videoSearchedTerms.csv")
    # Report - objective #2
    #abrir el archivo con la lista de videos
    File = 'Output/videoSummary_UED.csv'
    df_videosId = pd.read_csv(File, sep = '|')
    #eliminar videos repetidos f(videoId)
    df_videosId = df_videosId.drop_duplicates()
    #Generar reporte para cada video
    df_videosId['objetivo2'] = df_videosId.videoId.apply(objetivo2)
    #numero de videos en el archivo
    print("cantidad de videos unicos en el archivo: "+File+" , es: " + str(df_videosId.shape[0]))

    # Consolidar en un archivo los reportes por video del Objetivo2
    allFiles = []
    allFiles = allFiles + glob.glob('Output/Objetivo2/Objetivo2_report_videoId*.csv')
    alldf = pd.DataFrame()
    base = 'Uniandes'
    in_line = 0
    to_line = 0
    df = pd.DataFrame()

    if len(allFiles) == 0:
        print('No files ...')
    else:
        FileName = 'Output/Objetivo2/Objetivo2_reportes_consolidados.csv'
        trx_file = FileName
        with open(trx_file, 'w', encoding='latin-1') as out_file:
            with open(allFiles[0], 'r', encoding='latin-1') as file:
                for line in file:
                    fields_header = line.split('|')
                    to_line += 1
                    # out_file.write("FileName"+','+"Columns"+','+','.join(fields_header))
                    out_file.write(','.join(fields_header))
                    break

            for data_file in allFiles:
                # print(data_file)
                with open(data_file, 'r', encoding='latin-1') as file:
                    if data_file.count('Objetivo2_report_') > 0:
                        next(file)
                    for line in file:
                        in_line += 1
                        if (in_line % 500000 == 0):
                            sys.stdout.write("Processed:\t" + str(in_line) + "\tOut:\t" + str(to_line) + '\n')
                            sys.stdout.flush()
                        fields = line.split('|')
                        to_line += 1
                        # out_file.write(data_file+','+str(line.count(',')+1)+','+','.join(fields))
                        out_file.write(','.join(fields))
        print("Consultar reporte consolidado en: " + FileName)

    #agrupar datos por dia y esquema de difusion
    FileObj2 = 'Output/Objetivo2/Objetivo2_reportes_consolidados.csv'
    df_obj2 = pd.read_csv(FileObj2, sep = ',')
    print(df_obj2.shape)
    df_obj2.head(3)

    ##Objetivo1

    #agrupar datos por dia y esquema de difusion
    FilevideosId = 'Output/videoSummary_UED.csv'
    colslj = ['videoId', 'esquemaDifusion']
    df_videosSummary = pd.read_csv(FilevideosId, sep = '|', usecols = colslj)
    print(df_videosSummary.shape)
    df_videosSummary.head(3)

    df_obj2join = df_obj2.merge(df_videosSummary, how = 'left', on = 'videoId')
    dateis = df_obj2join.day.min().replace("/","-")
    datefs = df_obj2join.day.max().replace("/","-")
    df_obj2join.to_csv("Output/Objetivo2/Objetivo2_reporte_"+str(dateis)+"_a_"+str(datefs)+".csv", index = False, sep = '|')
    df_obj2join.to_csv('Output/videoMetrics_UED.csv', index = False, sep = '|')
    df_obj2join.head()

    dfgroups = df_obj2join.groupby(['day',
                                'esquemaDifusion',
            ])[["estimatedMinutesWatched", "views", "shares", "likes", "dislikes", "subscribersGained", "subscribersLost",
               "comments", "averageViewDuration", "averageViewPercentage"  ]].sum().reset_index()

    print(dfgroups.shape)
    dateis = dfgroups.day.min().replace("/","-")
    datefs = dfgroups.day.max().replace("/","-")
    dfgroups.to_csv("Output/Objetivo1/Objetivo1_reporte_"+str(dateis)+"_a_"+str(datefs)+".csv", index = False, sep = '|')
    dfgroups.head()

    # definir rango de fechas para iterar las consultas
    inicio = datetime(2016, 1, 1)
    fin = datetime.now()
    lista_fechas = []

    for d in range((fin - inicio).days + 1):
        item = (inicio + timedelta(days=d)).strftime("%Y-%m-%d")
        if item[-2:] == '01':
            # print(item)
            lista_fechas.append(item)

    print(lista_fechas)

    # agrupar datos por dia y esquema de difusion
    FilevideosId = 'Output/videoSummary_UED.csv'
    colslj = ['videoId', 'publishedAt', 'esquemaDifusion']
    df_videosSummary = pd.read_csv(FilevideosId, sep='|', usecols=colslj)
    print(df_videosSummary.shape)
    df_videosSummary.head(3)


    inicio = datetime(2016,1,1)
    fin    = datetime.now()
    lista_fechas = []

    lista_fechas = [(inicio + timedelta(days=d)).strftime("%Y-%m-%d")
                        for d in range((fin - inicio).days + 1)]

    esquemas = list(df_videosSummary.esquemaDifusion.unique())
    for ed in esquemas:
        print(ed.replace(' ', '_'))
        df_ed = list(df_videosSummary[df_videosSummary.esquemaDifusion == ed]['videoId'])
        print(len(df_ed))
        ed = ed.replace(' ', '_')
        n = 0
        while (n < len(df_ed) ):
            listavideos = ','.join(df_ed[n:n+20])
            n= n+20
            print(n)
            oldinicio = str(inicio)[:10]
            for k in range (1,len(lista_fechas)):
                if lista_fechas[k][-2:] == '01':
                    iniciok=lista_fechas[k]
                    fink=lista_fechas[k-1]
                    #print (str(oldinicio)+' to '+str(fink))
                    # iterar consulta aqui!

                    #Crear reporte segun metricas y dimensiones
                    analytics_query_response = youtube_analytics.reports().query(
                        dimensions='insightTrafficSourceDetail',
                        endDate=fink,
                        filters="video=="+ listavideos +";insightTrafficSourceType==YT_SEARCH",
                        ids="channel==MINE",
                        maxResults=25,
                        metrics='views',
                        sort='-views',
                        startDate=oldinicio
                    ).execute()


                    #extraer el vector de columnas
                    cols = [None] * len(analytics_query_response.get("columnHeaders", []))
                    i=0
                    for column_header in analytics_query_response.get("columnHeaders", []):
                        cols[i] = column_header["name"]
                        i+=1
                    #extraer la matriz de filas
                    Matrix = [[0 for x in range(len(analytics_query_response.get("columnHeaders", [])))] for y in range(len(analytics_query_response.get("rows", [])))]
                    i,j= 0,0

                    for row in analytics_query_response.get("rows", []):
                        for value in row:
                            Matrix[i][j]=value
                            j+=1
                        i+=1
                        j=0
                    # Exporting a Dataset to CSV, without index and header, only if there are data
                    if Matrix:
                        print (str(oldinicio)+' to '+str(fink))
                        df = pd.DataFrame.from_records(Matrix, columns = cols )
                        df['esquema'] = ed
                        df['date'] = oldinicio
                        df.to_csv('Output/Objetivo3/Objetivo3_report_'+str(ed)+'_'+str(n)+'_'+str(oldinicio)+'.csv',index=False)
                        print(df.head())

                    oldinicio=iniciok

    # Consolidar en un archivo los reportes por video del Objetivo3
    allFiles = []
    allFiles = allFiles + glob.glob('Output/Objetivo3/Objetivo3_*.csv')
    print(len(allFiles))
    alldf = pd.DataFrame()
    base = 'Uniandes'
    in_line = 0
    to_line = 0
    df = pd.DataFrame()

    if len(allFiles) == 0:
        print('No files ...')
    else:
        FileName = 'Output/videoSearchedTerms.csv'
        trx_file = FileName
        with open(trx_file, 'w', encoding='latin-1') as out_file:
            with open(allFiles[0], 'r', encoding='latin-1') as file:
                for line in file:
                    fields_header = line.split('|')
                    to_line += 1
                    # out_file.write("FileName"+','+"Columns"+','+','.join(fields_header))
                    out_file.write(','.join(fields_header))
                    break

            for data_file in allFiles:
                # print(data_file)
                with open(data_file, 'r', encoding='latin-1') as file:
                    if data_file.count('Objetivo3_report_') > 0:
                        next(file)
                    for line in file:
                        in_line += 1
                        if (in_line % 500000 == 0):
                            sys.stdout.write("Processed:\t" + str(in_line) + "\tOut:\t" + str(to_line) + '\n')
                            sys.stdout.flush()
                        fields = line.split('|')
                        to_line += 1
                        # out_file.write(data_file+','+str(line.count(',')+1)+','+','.join(fields))
                        out_file.write(','.join(fields))
        print("Consultar reporte consolidado en: " + FileName)


    #agregar la fecha de publicacion y titulo del video
    FilevideosId = 'Output/videoSummary_UED.csv'
    colslj = ['videoId', 'esquemaDifusion']
    df_videosSummary = pd.read_csv(FilevideosId, sep = '|', usecols = colslj)
    print(df_videosSummary.shape)
    df_videosSummary.head(3)