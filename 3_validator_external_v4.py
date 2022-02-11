from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import boto3
import sys
from datetime import date
import csv

BUCKET_NAME_DEV = 'bucket-wz-aw-dev-euc-external-l0'
BUCKET_NAME_PROD = 'bucket-wz-aw-prod-euc-external-l0'
BUCKET_QUERIES_DEV = 'bucket-wz-aw-dev-euc-data-ingestion'
BUCKET_QUERIES_PROD = 'bucket-wz-aw-prod-euc-data-ingestion'
PATH_QUERIES = 'validator/queries/'

# REPORT OUTPUT 
df_resultado = []
# Create a schema for the dataframe
schema = StructType([
    StructField('interface', StringType(), True),
    StructField('partition', StringType(), True),
    StructField('operation', StringType(), True),
    StructField('status', StringType(), True),
    StructField('error', StringType(), True),
    StructField('date_gbr', StringType(), True)
])

schema_prueba = "zerebro_gbr"



# S3 CONFIGURATION
client = boto3.client('s3')
s3 = boto3.resource('s3')
date_gbr = str(date.today())

if sys.argv[1][2:] == "prod":
    bucket_name = BUCKET_NAME_PROD
    bucket_queries = BUCKET_QUERIES_PROD
elif sys.argv[1][2:] == "dev":
    bucket_name = BUCKET_NAME_DEV
    bucket_queries = BUCKET_QUERIES_DEV
else:
    sys.exit("INTRODUCE MODE --dev OR --prod AS SECOND ARGUMENT")
bucket_obj = s3.Bucket(bucket_name)


# CONFIGURATION INTERFACES TO VALIDATE
if sys.argv[2][2:] == "interfaces.csv":
    print("INTERFACES TO VALIDATE")
    filename = sys.argv[2][2:]
    file = open(filename, "r")
    reader = csv.reader(file)
    interfaces = []
    for line in reader:
        t=line[0]
        interfaces.append(t)
    print(interfaces)
    # CONFIGURATION PARTITIONS TO VALIDATE
    if sys.argv[3][2:] == "all":
        latest = "NO"
    else:
        latest = "YES"
else:
    print("VALIDATE ALL INTERFACES WITH QUERIES IN {}/{}".format(bucket_queries, PATH_QUERIES))
    result = client.list_objects(Bucket=bucket_queries, Prefix=PATH_QUERIES, Delimiter='/')
    interfaces = []
    for o in result.get('CommonPrefixes'):
        interfaces.append(o.get('Prefix').replace(PATH_QUERIES,"")[:-1])
    # CONFIGURATION PARTITIONS TO VALIDATE
    if sys.argv[2][2:] == "all":
        latest = "NO"
    elif sys.argv[2][2:] == "latest":
        latest = "YES"
    else:
        sys.exit("INTRODUCE FILE --interfaces.csv AS SECOND ARGUMENT OR INTRODUCE --all OR --latest AS SECOND ARGUMENT IF YOU NEED TO WORK WITH ALL INTERFACES")


# CONFIGURATION SPARK APPLICATION
appName = "Validator_external"
master = "local"
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .enableHiveSupport() \
    .getOrCreate()


# VALIDATOR EXTERNAL
spark.sql("create database if not exists zerebro_gbr")
#Para cada interface, ejecutar las queries create external y alter table 
for interface in interfaces:

    dates = []
    prefix='DM/{}/'.format(interface)

    for obj in bucket_obj.objects.filter(Prefix=prefix):
        file = '{0}'.format(obj.key)

        #Limpieza de los directorios para poder hacer la busqueda de las fechas correctamente
        #Al hacer el split('_'), tendremos que quitar los ('_') que no nos interesan para no tener que cambiar el resto del proceso
        file = file.replace("dat_exec_year=", "")
        file = file.replace("dat_exec_month=", "")
        file = file.replace("dat_exec_day=", "")

        try:
            if "ODATE" in file: #PUEDE SER POR FDATE, ODATE
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
                
                dates.append(final_date)
        except:
                pass

    
    try: 
        #Habra que coger la fecha correspondiente a la particion correcta
        date_max = max(dates)
        # latest == "NO"
        #Se hara el proceso del alter tantas veces como fechas haya en los ficheros de origen, ya que habra que añadir dichas particiones
        #Definimos una lista con las fechas existentes sin que esten repetidas
        #Ordenamos la lista de menor a mayor para coger la fecha mas reciente primero (se coge cuando vamos a ejecutar el ALTER)
        # latest == "YES"
        #Se hara el proceso tan solo con la ultima particion 
        if latest == "YES":
            new_dates = [date_max]
        else: #latest == "NO"
            new_dates = list(set(dates))
            new_dates.sort()

        times = len(new_dates)
        
        #Puede que haya varios create_external.sql, por lo tanto habra que recoger todos
        i = 1
        while i != 20:
            
            if i == 1:
                #Ruta archivos queries
                queries_external = '{queries_path}{interface}/create_external_{interface}.sql'.format(queries_path = PATH_QUERIES, interface = interface)
                table_name = "et_{}".format(interface)

                try: #Por si no existe el CREATE, que no pare de ejecutar
                    
                    
                    #Acceso archivo queries_external
                    obj = s3.Object(bucket_queries, queries_external)

                    #Lectura y conversión a string
                    initial_query = (obj.get()['Body'].read().decode('utf-8'))  
                    

                    
                    initial_query = initial_query.replace("zerebro_external", "if not exists " + schema_prueba)
                    i += 1 
                
                #En caso de no existir el CREATE, guardar el log del error
                except Exception as e:
                    error = str(e)
                    resultado = (interface, "*", "No hay CREATE EXTERNAL para esta interface", "KO", error, date_gbr) 
                    df_resultado.append(resultado)
                    break

            else:
                queries_external = '{queries_path}{interface}/create_external_{interface}_{n}.sql'.format(queries_path = PATH_QUERIES, interface = interface, n = i)
                table_name = "et_{}_{}".format(interface, i)
            
                #Lectura QUERY CREATE EXTERNAL
            
                try: #Por si no existe el CREATE, que no pare de ejecutar
                    
                    
                    #Acceso archivo queries_external
                    obj = s3.Object(bucket_queries, queries_external)

                    #Lectura y conversión a string
                    initial_query = (obj.get()['Body'].read().decode('utf-8'))  

                    
                    initial_query = initial_query.replace("zerebro_external","if not exists " + schema_prueba)
                    i += 1
                    
                
                #En caso de no existir el CREATE seguir el proceso
                except Exception as e:
                    break



            #Ejecucion querie create_external de esta interface

            try:
                spark.sql("""{}""".format(initial_query))
                #status por defecto en caso de que no haya problemas en la ejecución
                error = "OK"
                resultado = (interface, "*","CREATE EXTERNAL", "OK", "No hay error", date_gbr) 
                df_resultado.append(resultado)
                
                
            except Exception as e:
                error = str(e)
                resultado = (interface, "*","CREATE EXTERNAL", "KO", error, date_gbr) 
                df_resultado.append(resultado)
                pass
            
            #Ejecucion del alter table para cada una de las particiones
            for time in range(times):
                date_part = new_dates[-1]
                del new_dates[-1]
                print('La fecha de la particion es: ', date_part)

                year = date_part[:4]
                month = date_part[4:6]
                day = date_part[6:8]    
                try:
                    spark.sql("""ALTER TABLE {schema_prueba}.{table} 
ADD PARTITION (dat_exec_year={ano},dat_exec_month={mes},dat_exec_day={dia}) 
LOCATION 's3a://{bucket}/DM/{num_interface}/dat_exec_year={ano}/dat_exec_month={mes}/dat_exec_day={dia}/';""".format(bucket = bucket_name, schema_prueba=schema_prueba,
                                                                                                                        table=table_name, 
                                                                                                                        num_interface = interface,
                                                                                                                        ano=year,
                                                                                                                        mes=month,dia=day))

                    
                    
                    select = spark.sql("SELECT dat_exec_year from {}.et_{} where dat_exec_year={} and dat_exec_month={} and dat_exec_day={} limit 1".format(schema_prueba,interface,year,month,day)).take(1)
                    #En caso de que encontremos datos en la particion en cuestion de la tabla external
                    if select:
                        error = 'OK'
                        print("ALTER EXITOSO")
                        resultado = (interface, date_part, "ALTER TABLE", 'OK', "No hay error", date_gbr)
                        df_resultado.append(resultado)
                       
                    else: #En caso de que no encontremos datos en la particion en cuestion de la tabla external
                        print("ALTER FAILED")
                        error = "Datos no cargados en tabla, fichero vacio"
                        resultado = (interface, date_part, "ALTER TABLE", "KO", error, date_gbr)
                        df_resultado.append(resultado)


                except Exception as e:
                    error = "ALTER mal implementado"
                    error_2 = str(e)
                    resultado = (interface, date_part, "ALTER TABLE", "KO", error, date_gbr)
                    df_resultado.append(resultado)
                    pass
            
                # Convert list to RDD
                rdd = spark.sparkContext.parallelize(df_resultado)

                # Create data frame
                df = spark.createDataFrame(rdd,schema)

                if latest == "NO":
                    df.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_EXTERNAL_{}_all".format(bucket_queries, date_gbr))
                else:
                    df.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_EXTERNAL_{}_latest".format(bucket_queries, date_gbr))

    except Exception as e:
        error_2 = str(e)
        error = "Lista de fechas vacia"
        resultado = (interface, "*", "No hay ficheros de datos en origen para esta interface", "KO", error, date_gbr) 
        df_resultado.append(resultado)
        pass

    print(interface + " EJECUTADA CON EXITO")

        
    
        
print("VALIDATOR_EXTERNAL EJECUTADO CON EXITO")
