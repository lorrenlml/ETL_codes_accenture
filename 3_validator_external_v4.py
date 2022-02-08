from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import boto3
import sys
from datetime import date
client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket_obj = s3.Bucket('bucket-wz-aw-dev-euc-external-l0')

appName = "Validator_external"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .enableHiveSupport() \
    .getOrCreate()

##FORMA MANUAL PASANDO LOS NUMEROS DE INTERFAZ QUE SE QUIEREN VALIDAR 
## total arguments
#n = len(sys.argv) - 1
#print("Total arguments passed:", n)
 
# Arguments passed
 
#print("\nArguments passed:")
#for i in range(n):
#    print(sys.argv[i + 1])
    
#interfases_list = []
#for i in range(n):
#    interfases_list.append(sys.argv[i + 1])

#print("La lista de interfases a utilizar es la siguiente:")
#print(interfases_list)

bucket = 'bucket-wz-aw-dev-euc-data-ingestion'
queries_path = 'validator/queries/'

#FORMA AUTOMATICA
result = client.list_objects(Bucket=bucket, Prefix=queries_path, Delimiter='/')

#Creacion lista de interfases/directorios
interfases_list = []
for o in result.get('CommonPrefixes'):
    interfases_list.append(o.get('Prefix').replace(queries_path,"")[:-1])

#print("La lista de interfases a utilizar es la siguiente:")
#print(interfases_list)

#Estructura de la lista/registro de logs
df_nombres = ['nombre_interfaz', 'operacion', 'error']

#Creacion lista/registro 
df_resultado = []
#Registro extendido
#df_resultado_2 = []
error = 'NO'
#, "20191" HA SIDO QUITADA DE LA LISTA POR OUT OF MEMORY
interfases_list = ["10002", "10003", "10026", "10029", "10030", "10031", "10308", "20265", "20275", "20277", 
"20285", "20386", "20387", "20388", "21059", "21351", "21356", "21710", "21711", "30223", "30307", "30721", "30722", 
"30723", "30726", "30727", "30728", "30729", "30747", "30751", "30755", "30757", "30758", "30759", "30760", "30761", 
"30762", "30763", "30765", "30766", "30767", "30768", "30773", "30774", "30775", "30776", "30777", "30778", "30779", 
"30780", "30781", "30782", "30783", "30784", "30785", "30786", "30789", "30791", "30792", "30798", "30802", "30806", 
"30817", "30818", "31060", "31061", "31062", "31065", "31094", "31095", "31096", "40049", "40050", "40380", "40382", 
"40383", "40385", "40386", "40387", "41059", "50351", "50358", "50359", "50360", "50363", "50364", "50365", "50366", 
"50368", "50369", "50370", "50371", "50372", "50373", "50374", "50377", "50379", "50380", "50381", "50382", "50383", 
"50384", "50390", "50440", "50603"]
#SOLO PARA PRUEBAS#####################################################

schema_prueba = "zerebro_gbr"
#spark.sql("drop database {} cascade".format(schema_prueba))
spark.sql("create database if not exists zerebro_gbr")
#Para cada interfase, ejecutar las queries create external y alter table 
#interfases_list = ["10003"]
for interfase in interfases_list:

    dates = []
    prefix='DM/{}/'.format(interfase)

    for obj in bucket_obj.objects.filter(Prefix=prefix):
        file = '{0}'.format(obj.key)

        #DM/10003/dat_exec_year=2021/dat_exec_month=06/dat_exec_day=24/ODATE_20210624_UPTPYO310003DI01.UAP.DSLDTRA.20210625161817.162426.IN
        

        #Limpieza de los directorios para poder hacer la busqueda de las fechas correctamente
        #Al hacer el split('_'), tendremos que quitar los ('_') que no nos interesan para no tener que cambiar el resto del proceso
        file = file.replace("dat_exec_year=", "")
        file = file.replace("dat_exec_month=", "")
        file = file.replace("dat_exec_day=", "")
        #print(file)
        #DM/10003/2021/06/24/ODATE_20210624_UPTPYO310003DI01.UAP.DSLDTRA.20210625161817.162426.IN

        try:
            if "ODATE" in file: #TENER CUIDADO CON ESTO PORQUE PUEDE SER POR FDATE, ODATE
                date = file.split('_')[1]

                if len(date) == 4:
                    month = "01"
                    day = "01"
                    year = date
                elif len(date) == 6:
                    day = "01"
                    month = date[4:6]
                    year = date[:4]
                else:
                    day = date[6:8]
                    month = date[4:6]
                    year = date[:4]

                final_date = year + month + day
                #print("fecha anadida es: ",final_date)
                dates.append(final_date)

            else:#FDATE
                date = file.split('_')[1].split(".")[0]

                if len(date) == 4:
                    month = "01"
                    day = "01"
                    year = date
                elif len(date) == 6:
                    day = "01"
                    month = date[4:6]
                    year = date[:4]
                else:
                    day = date[6:8]
                    month = date[4:6]
                    year = date[:4]

                final_date = year + month + day
                #print("fecha anadida es: ",final_date)
                dates.append(final_date)
        except:
                pass

    
    try: 
        #Habra que coger la fecha correspondiente a la particion correcta
        date_max = max(dates)
        #Se hara el proceso del alter tantas veces como fechas haya en los ficheros de origen, ya que habra que añadir dichas particiones
        #Definimos una lista con las fechas existentes sin que esten repetidas
        #Ordenamos la lista de menor a mayor para coger la fecha mas reciente primero (se coge cuando vamos a ejecutar el ALTER)
        new_dates = list(set(dates))
        new_dates.sort()
        times = len(new_dates)

        i = 1
        while i != 20:
            
            if i == 1:
                #Ruta archivos queries
                queries_external = '{queries_path}{interfase}/create_external_{interfase}.sql'.format(queries_path = queries_path, interfase = interfase)
                table_name = "et_{}".format(interfase)

                try: #Por si no existe el CREATE, que no pare de ejecutar
                    
                    
                    #Acceso archivo queries_external
                    obj = s3.Object(bucket, queries_external)

                    #Lectura y conversión a string
                    initial_query = (obj.get()['Body'].read().decode('utf-8'))  

                    #SOLO PARA PRUEBAS###############################################################
                    initial_query = initial_query.replace("zerebro_external", "if not exists " + schema_prueba)
                    i += 1 
                
                #En caso de no existir el CREATE, guardar el log del error
                except Exception as e:
                    error = str(e)
                    resultado = (interfase, "*", "No hay CREATE EXTERNAL para esta interface", "KO", error)
                    #resultado_2 = (interfase, "*", "No hay CREATE EXTERNAL para esta interfase", "KO", error) 
                    df_resultado.append(resultado)
                    #df_resultado_2.append(resultado_2)
                    break

            else:
                queries_external = '{queries_path}{interfase}/create_external_{interfase}_{n}.sql'.format(queries_path = queries_path, interfase = interfase, n = i)
                table_name = "et_{}_{}".format(interfase, i)
            
                #Lectura QUERY CREATE EXTERNAL
            
                try: #Por si no existe el CREATE, que no pare de ejecutar
                    
                    
                    #Acceso archivo queries_external
                    obj = s3.Object(bucket, queries_external)

                    #Lectura y conversión a string
                    initial_query = (obj.get()['Body'].read().decode('utf-8'))  

                    #SOLO PARA PRUEBAS###############################################################
                    initial_query = initial_query.replace("zerebro_external","if not exists " + schema_prueba)
                    i += 1
                    
                
                #En caso de no existir el CREATE, guardar el log del error
                except Exception as e:
                    break



            #Ejecucion querie create_external de esta interfase

            try:
                spark.sql("""{}""".format(initial_query))
                #Error por defecto en caso de que no haya problemas en la ejecución
                error = "OK"
                resultado = (interfase, "*","CREATE EXTERNAL", error, "No hay error") 
                #resultado_2 = (interfase, "*","CREATE EXTERNAL", error, "No hay error")
                df_resultado.append(resultado)
                #df_resultado_2.append(resultado_2)
                
                
            except Exception as e:
                error = str(e)
                resultado = (interfase, "*","CREATE EXTERNAL", "KO", error) 
                #resultado_2 = (interfase, "*","CREATE EXTERNAL", "KO", error)
                df_resultado.append(resultado)
                #df_resultado_2.append(resultado_2)
                pass
            
            #Ejecucion del alter table 
            for time in range(times):
                date_part = new_dates[-1]
                del new_dates[-1]


                print('La fecha maxima en el directorio es: ', date_part)

                #print(dates)
                #exec_date
                year = date_part[:4]
                month = date_part[4:6]
                day = date_part[6:8]    
                #print(year + "," + month + "," + day)
                try:
                    spark.sql("""ALTER TABLE {schema_prueba}.{table} 
ADD PARTITION (dat_exec_year={ano},dat_exec_month={mes},dat_exec_day={dia}) 
LOCATION 's3a://bucket-wz-aw-dev-euc-external-l0/DM/{num_interfase}/dat_exec_year={ano}/dat_exec_month={mes}/dat_exec_day={dia}/';""".format(schema_prueba=schema_prueba,table=table_name, 
                                                                                                                        num_interfase = interfase,
                                                                                                                        ano=year,
                                                                                                                        mes=month,dia=day))

                    
                    
                    select = spark.sql("SELECT dat_exec_year from {}.et_{} where dat_exec_year={} and dat_exec_month={} and dat_exec_day={} limit 1".format(schema_prueba,interfase,year,month,day)).take(1)
                    #print("este es el output", select)
                    if select:
                        error = 'OK'
                        print("ALTER EXITOSO")
                        resultado = (interfase, date_part, "ALTER TABLE", error, "No hay error")
                        #resultado_2 = (interfase, date_part, "ALTER TABLE", error, "Comprobacion con SELECT satisfactoria")
                        df_resultado.append(resultado)
                        #df_resultado_2.append(resultado_2)
                    else:
                        print("ALTER FAILED")
                        #error = "Coge mal exec_date porque coge letras/signos en vez de numeros debido a que el nombre del fichero origen tiene formato no deseado"
                        error = "Datos no cargados en tabla, fichero vacio"
                        resultado = (interfase, date_part, "ALTER TABLE", "KO", error)
                        #resultado_2 = (interfase, date_part, "ALTER TABLE", "KO", error)
                        df_resultado.append(resultado)
                        #df_resultado_2.append(resultado_2)
                    
                    #error = "OK"
                    #resultado = (interfase, "ALTER TABLE", error, "No hay error")
                    #df_resultado.append(resultado)
                except Exception as e:
                    #error = "Coge mal exec_date porque coge letras/signos en vez de numeros debido a que el nombre del fichero origen tiene formato no deseado"
                    error = "ALTER mal implementado"
                    error_2 = str(e)
                    #resultado_2 = (interfase, "ALTER TABLE {}".format(date_part), "KO", e)
############################################################################################################## HACER SEGUNDO REPORT CON ERROR EXTENDIDO
                    resultado = (interfase, date_part, "ALTER TABLE", "KO", error)
                    #resultado_2 = (interfase, date_part, "ALTER TABLE", "KO", error_2)
                    df_resultado.append(resultado)
                    #df_resultado_2.append(resultado_2)
                    pass
    
    except Exception as e:
        error_2 = str(e)
        error = "Lista de fechas vacia"
        resultado = (interfase, "*", "No hay ficheros de datos en origen para esta interface", "KO", error) 
        #resultado_2 = (interfase, "*", "No hay ficheros de datos en origen para esta interface", "KO", error_2)
        df_resultado.append(resultado)
        #df_resultado_2.append(resultado_2)
        pass

    print(interfase + " EJECUTADA CON EXITO")

        
    
        
print("VALIDATOR_EXTERNAL EJECUTADO CON EXITO")

# Create a schema for the dataframe
schema = StructType([
    StructField('nombre_interfaz', StringType(), True),
    StructField('partition', StringType(), True),
    StructField('operacion', StringType(), True),
    StructField('OK/KO', StringType(), True),
    StructField('error', StringType(), True)
])

# Convert list to RDD
rdd = spark.sparkContext.parallelize(df_resultado)

# Create data frame
df = spark.createDataFrame(rdd,schema)


df.coalesce(1).write.mode("overwrite").option("delimiter","|").format("csv").save("s3://bucket-wz-aw-dev-euc-data-ingestion/validator/report_EXTERNAL")

#ERROR EXTENDIDO 
# Create a schema for the dataframe
#schema_2 = StructType([
#    StructField('nombre_interfaz', StringType(), True),
#    StructField('partition', StringType(), True),
#    StructField('operacion', StringType(), True),
#    StructField('OK/KO', StringType(), True),
#    StructField('error', StringType(), True)
#])

# Convert list to RDD
#rdd_2 = spark.sparkContext.parallelize(df_resultado_2)

# Create data frame
#df_2 = spark.createDataFrame(rdd_2,schema_2)


#df_2.coalesce(1).write.mode("overwrite").option("delimiter","|").format("csv").save("s3://bucket-wz-aw-dev-euc-data-ingestion/validator/report_EXTERNAL_4")