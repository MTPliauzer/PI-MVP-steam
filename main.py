from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np

app=FastAPI()

@app.get('/')
class Inputs(BaseModel): #clase para validacion de datos
    genero1: str                                                #Especifico los tipo de datos para cada variable de cada query:
    genero2: str
    año1: int
    año2: int
    año3: int
def Post():                                                     #Funcion que ejecuta todas las demas funciones
    PlayTimeGenre(genero1)
    UserForGenre(genero2)
    UsersRecommend(año1)
    UsersNotRecommend(año2)
    sentiment_analysis(año3)

#creo 2da ruta:
@app.post("/PlayTimeGenre")                                                                             #Entrada especifica para solo la  1era Funcion
def PlayTimeGenre(genero: str):

    i_dfnew=[] 
    mask=[i_dfnew.append(e) for e,i in enumerate(df_games['genres']) if genero in df_games['genres'][e]] #Mascara para filtrar el df_games y solo quedarme con los row que contengan el genero que necesito.
    dfnew=df_games.iloc[i_dfnew]                                                                         #Creo el df que filtrado por el genero que necesito evaluar
    df_new=pd.merge(left=dfnew,right=df_items,left_on='id',right_on='item_id')                           #Uno el df filtrado por genero y el df_items(con las hs jugadas)
    df_query1=df_new.groupby(by=['release_date'], dropna=False).sum(['playtime_2weeks'])                 #Agrupo y sumo los valores por anio
    dfoutput=df_query1.index.where(df_query1['playtime_2weeks'] == df_query1['playtime_2weeks'].max())   #creo lis con Nones yel indice(es decir el anio) con la maxima cantidad de 'playtime_2weeks' (minutos jugados)
    output_q1=[int(i) for i in dfoutput if i != None]                                                    #Extraigo el indice(es decir el Anio),pero en forma de list :(
    output_q1=output_q1[0]                                                                               #extraigo el primer(y unico) valor de la list(uotput_q1) y lo declaro como uotput_q1 
    return {'Año de lanzamiento con más horas jugadas para Género X:': output_q1}
    
@app.post('/UserForGenre')                                                                               ##Entrada especifica para solo la 2da Funcion
def UserForGenre(genero: str):
    i_dfnew=[] 
    mask=[i_dfnew.append(e) for e,i in enumerate(df_games['genres']) if genero in df_games['genres'][e]] #Mascara para filtrar el df_games y solo quedarme con los row que contengan el genero que necesito.
    dfnew=df_games.iloc[i_dfnew]                                                                         #Creo el df que filtrado por el genero que necesito evaluar
    df_new=pd.merge(left=dfnew,right=df_items,left_on='id',right_on='item_id')                           #Uno el df filtrado por genero y el df_items(con las hs jugadas)
    df_query2=df_new.groupby(by=['user_id'], dropna=False).sum(['playtime_forever'])                     #Agrupo y sumo los valores por usuario(user_id)
    dfoutput=df_query2.index.where(df_query2['playtime_forever'] == df_query2['playtime_forever'].max()) #creo lis con Nones y el numero del indice(del usuario, user_id) con la maxima cantidad de 'playtime_forever' (minutos jugados acumulados)
    output_q2=[e for i,e in enumerate(dfoutput) if e!= None][0]        
    i_dfnew2=[] 
    mask=[i_dfnew2.append(e) for e,i in enumerate(df_items['user_id']) if output_q2 in df_items['user_id'][e]] #Mascara para filtrar el df_games y solo quedarme con los row que contengan el genero que necesito.
    dfnew=df_items.iloc[i_dfnew2]                                                                         #Creo el df que filtrado por el usuario que necesito evaluar(que el que tiene + hs jugadas por el genero elegido)
    df_new=pd.merge(left=df_games,right=dfnew,left_on='id',right_on='item_id')                                 #Uno los df_games y df recien filtrado por usuario
    df_new=df_new.groupby(['release_date']).sum(['playtime_forever'])                                          #Agrupo y sumo por Anio
    df_new=df_new.reset_index()
    lis=[]
    dicc=dict()
    for e,i in enumerate(df_new['id']):
        Anio= int(df_new['release_date'][e])
        Hs=df_new['playtime_forever'][e]
        dicc={'Año':Anio,'Horas':Hs}
        lis.append(dicc)
    return{"Usuario con más horas jugadas para Género{genero}": output_q2,"Horas jugadas":lis}

''' 3era Query'''
@app.post('/UsersRecommend')                                                                                #Entrada especifica para solo la 3era Funcion
def UsersRecommend(año:int):
    año=int(input())
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


''' 4ta QUERY'''
@app.post('/UsersNotRecommend')                                                                               #Entrada especifica para solo 4ta la Funcion
def UsersNotRecommend(año:int):
    #año=int(input())
    i_dfnew=[]
    i_pos=[]
    i_po=[]
    output_q3=[]
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
    return{output_q4}

''' 5ta QUERY'''
@app.post('/sentiment_analysis')                                                                             #Entrada especifica para solo la 5ta Funcion
def sentiment_analysis(año:int):
    #año=int(input())
    i_dfnew=[]
    mask=[i_dfnew.append(e) for e,i in enumerate(df_games['release_date']) if df_games['release_date'][e]== str(año)] #Mascara para filtrar el df_games y solo quedarme con los row que contengan el anio que necesito.
    dfnew=df_games.iloc[i_dfnew]
    df_new=pd.merge(left=dfnew,right=df_items,left_on='id',right_on='item_id')                                        #Uno los df_items con el df_games filtrado por anio
    df_new=pd.merge(left=df_new,right=df_reviews,left_on='item_id',right_on='item_id')                                #Uno mi actual df filtrado con df_reviews
    di=dict(df_new.sentiment_analysis.value_counts())                                                                 #Creo un diccionario para imprimir con el formato como pide el enunciado, las tres cantidades +,=,- de reviews
    di['Negative']=di.pop(0)                                                                                          #ordeno y renombro las claves del diccionario.
    di['Neutral']=di.pop(1)
    di['Positive']=di.pop(2)
    return {di}
