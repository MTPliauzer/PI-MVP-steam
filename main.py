from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np

df_games=pd.read_csv('games.CSV',sep=';')                       #achivos de texto (CSV) que voy a utilizar
df_items=pd.read_csv('items.CSV',sep=';')
df_reviews=pd.read_csv('reviews.CSV',sep=';')


app=FastAPI()
#http://127.0.0.1:8000/ ---->Ruta para ejecutar en el navegador y ver mi api
@app.get("/")
def Post():                                                     #Funcion que ejecuta todas las demas funciones
    return 'API de PI-MVP-Steam. Para ejecutar las 5 consultas, insertar en el navegador las siguientes rutas: "http://127.0.0.1:8000/":  1."/PlayTimeGenre/{genero}", 2."/UserForGenre/{genero}", 3."/UsersRecommend/{año}",4."/UsersNotRecommend/{año}" , 5."/sentiment_analysis/{año}"'
#creo 1ra ruta especifica:
@app.get("/PlayTimeGenre/{genero}")                                                                             #Entrada especifica para solo la  1era Funcion
def PlayTimeGenre(genero):
    i_dfnew=[] 
    mask=[i_dfnew.append(e) for e,i in enumerate(df_games['genres']) if genero in df_games['genres'][e]] #Mascara para filtrar el df_games y solo quedarme con los row que contengan el genero que necesito.
    dfnew=df_games.iloc[i_dfnew]                                                                         #Creo el df que filtrado por el genero que necesito evaluar
    df_new=pd.merge(left=dfnew,right=df_items,left_on='id',right_on='item_id')                           #Uno el df filtrado por genero y el df_items(con las hs jugadas)
    df_query1=df_new.groupby(by=['release_date'], dropna=False).sum(['playtime_2weeks'])                 #Agrupo y sumo los valores por anio
    dfoutput=df_query1.index.where(df_query1['playtime_2weeks'] == df_query1['playtime_2weeks'].max())   #creo lis con Nones yel indice(es decir el anio) con la maxima cantidad de 'playtime_2weeks' (minutos jugados)
    output_q1=[int(i) for i in dfoutput if i != None]                                                    #Extraigo el indice(es decir el Anio),pero en forma de list :(
    output_q1=output_q1[0]                                                                               #extraigo el primer(y unico) valor de la list(uotput_q1) y lo declaro como uotput_q1 
    return {'Año de lanzamiento con más horas jugadas para Género X': output_q1}
#creo 2da ruta especifica:
    
@app.get("/UserForGenre/{genero}")                                                                               ##Entrada especifica para solo la 2da Funcion
def UserForGenre(genero): 
    i_dfnew=[] 
    mask=[i_dfnew.append(e) for e,i in enumerate(df_games['genres']) if str(genero) in df_games['genres'][e]] #Mascara para filtrar el df_games y solo quedarme con los row que contengan el genero que necesito.
    dfnew=df_games.iloc[i_dfnew]                                                                         #Creo el df que filtrado por el genero que necesito evaluar
    df_new=pd.merge(left=dfnew,right=df_items,left_on='id',right_on='item_id')                           #Uno el df filtrado por genero y el df_items(con las hs jugadas)
    df_query2=df_new.groupby(by=['user_id'], dropna=False).sum(['playtime_forever'])                     #Agrupo y sumo los valores por usuario(user_id)
    dfoutput=df_query2.index.where(df_query2['playtime_forever'] == df_query2['playtime_forever'].max()) #creo lis con Nones y el numero del indice(del usuario, user_id) con la maxima cantidad de 'playtime_forever' (minutos jugados acumulados)
    output_q2lis=[int(i) for i,e in (enumerate(dfoutput)) if e != None]
    df_query2=df_query2.iloc[output_q2lis]
    df_query2=df_query2.reset_index()
    output_q2=df_query2['user_id'][0]
    i_dfnew2=[] 
    mask=[i_dfnew2.append(e) for e,i in enumerate(df_items['user_id']) if str(output_q2) in df_items['user_id'][e]] #Mascara para filtrar el df_games y solo quedarme con los row que contengan el genero que necesito.
    dfnew=df_items.iloc[i_dfnew2]                                                                         #Creo el df que filtrado por el usuario que necesito evaluar(que el que tiene + hs jugadas por el genero elegido)
    df_new=pd.merge(left=df_games,right=dfnew,left_on='id',right_on='item_id')                                 #Uno los df_games y df recien filtrado por usuario
    df_new=df_new.groupby(['release_date']).sum(['playtime_forever'])                                          #Agrupo y sumo por Anio
    df_new=df_new.reset_index()
    lis=[]
    for e,i in enumerate(df_new['id']):
        Anio= int(df_new['release_date'][e])
        Hs=df_new['playtime_forever'][e]
        dicc={'Año':Anio,'Horas':Hs}
        lis.append(dicc)
    return{"Usuario con más horas jugadas para Género{genero}": output_q2,"Horas jugadas":lis}

#creo 3ra ruta especifica:
@app.get("/UsersRecommend/{año}")                                                                                #Entrada especifica para solo la 3era Funcion
def UsersRecommend(año:int):
    i_dfnew=[]
    i_pos=[]
    i_po=[]
    output_q3=[]
    mask=[i_dfnew.append(e) for e,i in enumerate(df_reviews['posted']) if df_reviews['posted'][e]== año] #Mascara para filtrar el df y solo quedarme con los row que contengan el anio que necesito.
    dfnew=df_reviews.iloc[i_dfnew]
    maskpos=[i_pos.append(e) for e,i in enumerate(dfnew['recommend']) if i == True]                          #Solo me quedo con los rows de juegos que si fueron recomendados
    dfnew=dfnew.iloc[i_pos]
    maskpos=[i_po.append(e) for  e,i in enumerate(dfnew['sentiment_analysis']) if i >= 1]                    #Solo los rows calificados como positivos(+) o neutrales(=)
    dfnew=dfnew.iloc[i_po]
    df_item=df_items[['item_id','item_name']]
    df_new=pd.merge(left=dfnew,right=df_item,left_on='item_id',right_on='item_id')                           #Uno el df_reviews filtrado por anio y el df_items.
    df_query3=df_new.groupby(by=['item_name'], dropna=False).sum(['sentiment_analysis'])                     #Agrupo y sumo los valores por 'sentiment_analysis'
    df_query3=df_query3.sort_values(by=['sentiment_analysis'], ascending=False)
    df_query3=df_query3.reset_index()
    dicc={'Puesto 1':df_query3.item_name[0],'Puesto 2':df_query3.item_name[1],'Puesto 3':df_query3.item_name[2]}
    output_q3.append(dicc)
    return{output_q3}

@app.get("/UsersNotRecommend/{año}")
def UsersNotRecommend(año:int):
    i_dfnew=[]
    i_pos=[]
    i_po=[]
    output_q4=[]
    mask=[i_dfnew.append(e) for e,i in enumerate(df_reviews['posted']) if df_reviews['posted'][e]== año] #Mascara para filtrar el df y solo quedarme con los row que contengan el anio que necesito.
    dfnew=df_reviews.iloc[i_dfnew]
    maskpos=[i_pos.append(e) for e,i in enumerate(dfnew['recommend']) if i == False ]                        #Solo me quedo con los rows de juegos que no fueron recomendados
    dfnew=dfnew.iloc[i_pos]
    maskpos=[i_po.append(e) for  e,i in enumerate(dfnew['sentiment_analysis']) if i == 0 ]                   #Solo los rows calificados como negativos(-)
    dfnew=dfnew.iloc[i_po]
    df_item=df_items[['item_id','item_name']]
    df_new=pd.merge(left=dfnew,right=df_item,left_on='item_id',right_on='item_id')                           #Uno el df_reviews filtrado por anio y el df_items.
    df_query4=df_new.groupby(by=['item_name'], dropna=False).sum(['sentiment_analysis'])                     #Agrupo y sumo los valores por 'sentiment_analysis'
    df_query4=df_query4.sort_values(by=['sentiment_analysis'], ascending=True)
    df_query4=df_query4.reset_index()
    dicc={'Puesto 1':df_query4.item_name[0],'Puesto 2':df_query4.item_name[1],'Puesto 3':df_query4.item_name[2]}
    output_q4.append(dicc)
    return output_q4

#creo 5ta ruta especifica:
@app.get('/sentiment_analysis/{año}')                                                                             #Entrada especifica para solo la 5ta Funcion
def sentiment_analysis(año):
    i_dfnew=[]
    mask=[i_dfnew.append(e) for e,i in enumerate(df_games['release_date']) if df_games['release_date'][e]== str(año)] #Mascara para filtrar el df_games y solo quedarme con los row que contengan el anio que necesito.
    dfnew=df_games.iloc[i_dfnew]
    df_new=pd.merge(left=dfnew,right=df_items,left_on='id',right_on='item_id')                                        #Uno los df_items con el df_games filtrado por anio
    df_new=df_new[['item_id','release_date']]
    dff=pd.merge(left=df_new,right=df_reviews,left_on='item_id',right_on='item_id')                            #Uno mi actual df filtrado con df_reviews
    di=dict(dff.sentiment_analysis.value_counts())
    di['Negative']=di.pop(0)                                                                                          #ordeno y renombro las claves del diccionario.
    di['Neutral']=di.pop(1)
    di['Positive']=di.pop(2)
    return di
