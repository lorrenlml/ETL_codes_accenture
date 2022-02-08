from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import boto3
import sys
import os
from datetime import date
import os
import subprocess
client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket_obj = s3.Bucket('bucket-wz-aw-dev-euc-external-l0')
insert_date = str(date.today())
appName = "Validator_ods"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .enableHiveSupport() \
    .getOrCreate()

bucket = 'bucket-wz-aw-dev-euc-data-ingestion'
queries_path = 'validator/queries/'


#Creacion lista/registro 
df_resultado_n = [] #registro extendido que se genera con cada una de las particiones de las interfaces. Se genera un report para cada interfaz

#TODAS LAS INTERFACES
interfases_list = ["10002", "10003", "10026", "10029", "10030", "10031", "10308", "20191", "20265", "20275", "20277", 
"20285", "20386", "20387", "20388", "21059", "21351", "21356", "21710", "21711", "30223", "30307", "30721", "30722", 
"30723", "30726", "30727", "30728", "30729", "30747", "30751", "30755", "30757", "30758", "30759", "30760", "30761", 
"30762", "30763", "30765", "30766", "30767", "30768", "30773", "30774", "30775", "30776", "30777", "30778", "30779", 
"30780", "30781", "30782", "30783", "30784", "30785", "30786", "30789", "30791", "30792", "30798", "30802", "30806", 
"30817", "30818", "31060", "31061", "31062", "31065", "31094", "31095", "31096", "40049", "40050", "40380", "40382", 
"40383", "40385", "40386", "40387", "41059", "50351", "50358", "50359", "50360", "50363", "50364", "50365", "50366", 
"50368", "50369", "50370", "50371", "50372", "50373", "50374", "50377", "50379", "50380", "50381", "50382", "50383", 
"50384", "50390", "50440", "50603"]

#INTERFACES QUE HAN PASADO EL VALIDATOR EXTERNAL
passed_gbr_ext = ["10002", "10003", "20386", "20387", "20388", "21059", "21351", "21356", "21710", "21711", "30223", 
"30721", "30722", "30723", "30726", "30727", "30728", "30765", "30767", "30777", "30779", "30781", "30784", "31096", "40049", "40050", 
"40380", "40382", "40383", "40385", "40387", "41059", "50358", "50359", "50360", "50363", "50364", "50365", "50440"]

# Tener en cuenta que se ha quitado esta "20191",

schema_prueba = "zerebro_gbr_ods"
schema_prueba_ext = "zerebro_gbr"
spark.sql("drop database {} cascade".format(schema_prueba))
#SOLO PARA PRUEBAS ESTOS ESQUEMAS
spark.sql("create database if not exists {}".format(schema_prueba))

schema_n = StructType([
    StructField('interface', StringType(), True),
    StructField('partition', StringType(), True),
    StructField('operation', StringType(), True),
    StructField('status', StringType(), True),
    StructField('action_realized', StringType(), True),
    StructField('error_explanation', StringType(), True),
    StructField('date_gbr', StringType(), True)
])


#Para cada interface, ejecutar las queries create ods e insert
for interfase in passed_gbr_ext:
    print(interfase)

    
    #EJECUCION CREATES ODS
    i = 1
    #Bucle para coger varios archivos de creacion de ods en caso de que los haya
    while i != 20:
        
        if i == 1:
            #Ruta archivos queries ods en caso de que sea el primer create que se coge
            queries_ods = '{queries_path}{interfase}/create_ods_{interfase}.sql'.format(queries_path = queries_path, interfase = interfase)

        else:
            #Ruta archivos queries ods en caso de que no sea el primer create que se coge
            queries_ods = '{queries_path}{interfase}/create_ods_{interfase}_{n}.sql'.format(queries_path = queries_path, interfase = interfase, n = i)

        creates_ods = []
        try: #Por si no existe el CREATE, que no pare de ejecutar
            
            
            #Acceso archivo queries_ods
            obj = s3.Object(bucket, queries_ods)

            #Lectura y conversion a string
            initial_query = (obj.get()['Body'].read().decode('utf-8'))

            initial_query = initial_query.replace("zerebro_ods", "if not exists " + schema_prueba)

            #Dividir las queries por si hay varias en el mismo fichero
            queries = initial_query.split(";")
            
            #Coger solo las queries que nos interesan, con CREATE
            for j in range(len(queries)):
                if "CREATE" in queries[j]:
                    creates_ods.append(queries[j])
                #Contemplar caso en el que aparece TERMINATED BY ';' ya que se separaría la query
                if len(queries[j]) > 0:
                    if queries[j][0] == "'":
                        creates_ods[-1] = creates_ods[-1] + ";" + queries[j]


        #En caso de no existir el CREATE, guardar el log del error
        except Exception as e:
            if i == 1: #Si se esta cogiendo el primer create y falla, habra que generar un reporte de error
                error = str(e)
                resultado_n = (interfase, "*","No hay CREATE ODS para esta interface", "KO", "*","*", insert_date)
                df_resultado_n.append(resultado_n)
                break
            else: #Si se esta intentando coger otro create y no hay, no pasa nada
                break

        if len(creates_ods) > 0: #Para recoger todos los creates ods, ya que algunas interfaces tienen varios
            try:
                #PROCESO DE LANZAR CREATES
                for k in range(len(creates_ods)):

                    spark.sql("""{}""".format(creates_ods[k]))
                    print("CREATE ODS {}".format(k + 1) + " hecho")
                    error = "OK"
                    resultado_n = (interfase, "*","CREATE ODS {}".format(k + 1), "OK", "*","*", insert_date)
                    df_resultado_n.append(resultado_n)


            except Exception as e:
                error = str(e)
                resultado_n = (interfase, "*","CREATE ODS", "KO", error, "*",insert_date)
                df_resultado_n.append(resultado_n)
                pass


        i += 1 
    
    #EJECUCION INSERTS

    #Busqueda de country en el nombre de los ficheros
    country = "NOT_VALID"
    prefix='DM/{}/'.format(interfase)

    for obj in bucket_obj.objects.filter(Prefix=prefix):
        file = '{0}'.format(obj.key)
        #Limpieza de los directorios para poder hacer la busqueda de las fechas correctamente
        #Al hacer el split('_'), tendremos que quitar los ('_') que no nos interesan para no tener que cambiar el resto del proceso
        file = file.replace("dat_exec_year=", "")
        file = file.replace("dat_exec_month=", "")
        file = file.replace("dat_exec_day=", "")

        if "ODATE" in file: #TENER CUIDADO CON ESTO PORQUE PUEDE SER POR FDATE, ODATE
            try:
                country = file.split('_')[2][1:3] #Son las posiciones en las que se encuentra el country si es fichero .IN
                break
            except:
                pass
        
        else: #FDATE 
            try:
                country = file.split('_')[0][-21:-19] #Son las posiciones en las que se encuentra el country si no es fichero .IN
                break
            except:
                pass
    
    print('El pais es: ', country)
        

    r = 1
    while r != 20:
        if country == "NOT_VALID":

            break
        else:
        
            if r == 1:
                #Ruta archivos inserts en caso de que sea el primer insert que se coge
                queries_inserts = '{queries_path}{interfase}/insert_{interfase}.sql'.format(queries_path = queries_path, interfase = interfase)

            else:
                #Ruta archivos inserts en caso de que no sea el primer insert que se coge
                queries_inserts = '{queries_path}{interfase}/insert_{interfase}_{n}.sql'.format(queries_path = queries_path, interfase = interfase, n = r)

            inserts = []
            try: #Por si no existe el insert, que no pare de ejecutar
                
                
                #Acceso archivo inserts
                obj = s3.Object(bucket, queries_inserts)

                #Lectura y conversion a string
                initial_query = (obj.get()['Body'].read().decode('utf-8'))

                initial_query = initial_query.replace("zerebro_ods",schema_prueba)
                initial_query = initial_query.replace("zerebro_external",schema_prueba_ext)

                queries = initial_query.split(";")
                

                for j in range(len(queries)):
                    if "INSERT" in queries[j]:
                        inserts.append(queries[j])


            #En caso de no existir el INSERT, guardar el log del error
            except Exception as e:
                if r == 1: #Si se esta cogiendo el primer insert habra que generar un reporte de error
                    error = str(e)

                    resultado_n = (interfase, "*","No hay INSERT para esta interface", "KO", "*","*", insert_date)
                    df_resultado_n.append(resultado_n)
                    break
                else: #Si se esta intentando coger otro insert y no hay, no pasa nada
                    break

            if len(inserts) > 0:
                
                try:

                    #PROCESO DE LANZAR INSERTS
                    for k in range(len(inserts)):

                        new_dates = spark.sql('select (dat_exec_year || dat_exec_month || dat_exec_day) as exec_date from zerebro_gbr.et_{}'.format(interfase)). \
                        distinct().rdd.flatMap(lambda x: x).collect()
                        #Se hara el proceso del insert tantas veces como fechas haya en los ficheros de origen, ya que habra que añadir dichas particiones
                        #Definimos una lista con las fechas existentes sin que esten repetidas, cogiendolas de la tabla external
                        #Ordenamos la lista de menor a mayor para coger la fecha mas reciente primero
                        
                        new_dates.sort()
                        print("LA LISTA ORDENADA ES: ", new_dates)
                        times = len(new_dates)
                        #Contador latest para controlar la particion snapshot = 'LATEST' y que las demas sean 'HISTORIC'
                        latest = 1
                        for time in range(times):
                            date_part = new_dates[-1]
                            #Eliminamos la fecha que hemos cogido para no volverla a coger en la siguiente iteracion
                            del new_dates[-1]

                            print('La fecha maxima en el directorio es: ', date_part)

                            year = date_part[:4]
                            month = date_part[4:6]
                            day = date_part[6:8]   

                            #Insercion de los campos que nos interesan, ya que en el insert vienen por defecto como variables a introducir
                            inserts[k] = inserts[k].replace("'${Vinsert_DATE}'", "'{}' as insert_date".format(insert_date))
                            if latest == 1:
                                inserts[k] = inserts[k].replace("'${VSNAPSHOT}'", "'LATEST'")
                            else:
                                inserts[k] = inserts[k].replace("'${VSNAPSHOT}'", "'HISTORIC'")
                            inserts[k] = inserts[k].replace("'${VODATE}'", "{}".format(date_part))
                            inserts[k] = inserts[k].replace("'${VCOUNTRY}'", "'{}'".format(country))

                            if "${VODATE_YYYY}" in inserts[k]: 
                                inserts[k] = inserts[k].replace('${VODATE_YYYY}', year)
                                inserts[k] = inserts[k].replace('${VODATE_MM}', month)
                                inserts[k] = inserts[k].replace('${VODATE_DD}', day)

                            #Ejecucion de la querie desde hive, ya que con spark daba problemas el insert
                            #El problema era debido al cast que hay que hacer a la hora de hacer el insert y que no tenemos
                            #Este cast lo hace hive por defecto y por eso recurrimos a lanzar la querie desde aqui
                            os.system('hive -e "{}" -hiveconf hive.execution.engine=tez hive.exec.dynamic.partition=false'.format(inserts[k]))
                            
                            select = spark.sql("SELECT insert_date from {}.ods_{} where exec_date = {} limit 1".format(schema_prueba, interfase, date_part)).take(1)
                            #print("Este es el output", select)
                            #En caso de que encontremos datos en la particion en cuestion de la tabla ods
                            if select:
                                error = 'OK'
                                print("INSERT {} {}".format(k + 1, date_part) + " EXITOSO")
                                resultado_n = (interfase, date_part, "INSERT {}".format(k + 1), "OK", "INSERT EJECUTADO", "SELECT DEVUELVE PARTICION", insert_date)
                                df_resultado_n.append(resultado_n)
                                latest += 1
                            else: #En caso de que no encontremos datos en la particion en cuestion de la tabla ods
                                error = 'KO'
                                print("INSERT {} {}".format(k + 1, date_part) + " FALLIDO")
                                resultado_n = (interfase, date_part, "INSERT {}".format(k + 1), "KO", "INSERT EJECUTADO", "SELECT NO DEVUELVE NADA INSERT ERRONEO", insert_date)
                                df_resultado_n.append(resultado_n)
                                # Convert list to RDD
                                rdd_n = spark.sparkContext.parallelize(df_resultado_n)

                                # Create data frame
                                df_n = spark.createDataFrame(rdd_n,schema_n)

                                df_n.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://bucket-wz-aw-dev-euc-data-ingestion/validator/report_ODS/report_ODS_{}".format(interfase))
                                break

                            # Convert list to RDD
                            rdd_n = spark.sparkContext.parallelize(df_resultado_n)

                            # Create data frame
                            df_n = spark.createDataFrame(rdd_n,schema_n)


                            df_n.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://bucket-wz-aw-dev-euc-data-ingestion/validator/report_ODS/report_ODS_{}".format(interfase))

                except Exception as e:
                    error = str(e)
                    print(error)
                    resultado_n = (interfase, date_part,"INSERT", "KO", "INSERT NO EJECUTADO EN HIVE", error, insert_date)
                    df_resultado_n.append(resultado_n)
                    
                    pass


            r += 1 

        
print("VALIDATOR_ODS EJECUTADO CON EXITO")
