from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import boto3
import sys
from datetime import datetime
from datetime import date
import csv
import os
import argparse
try:    
    import ConfigParser
except:       
    import configparser as ConfigParser

BUCKET_NAME_DEV = 'bucket-wz-aw-dev-euc-external-l0'
BUCKET_NAME_PROD = 'bucket-wz-aw-prod-euc-external-l0'
BUCKET_QUERIES_DEV = 'bucket-wz-aw-dev-euc-data-ingestion'
BUCKET_QUERIES_PROD = 'bucket-wz-aw-prod-euc-data-ingestion'
PATH_QUERIES = 'validator/queries/'
schema_prueba = "zerebro_gbr"

def main(**kwargs):
    # REPORT OUTPUT 
    df_resultado = []
    # Create a schema for the dataframe
    schema = StructType([
        StructField('interface', StringType(), True),
        StructField('partition', StringType(), True),
        StructField('operation', StringType(), True),
        StructField('status', StringType(), True),
        StructField('error', StringType(), True),
        StructField('date_gbr', StringType(), True),
        StructField('count', StringType(), True)
    ])


    # S3 CONFIGURATION
    client = boto3.client('s3')
    s3 = boto3.resource('s3')
    date_gbr_run = str(date.today())

    env = kwargs["env"]
    if env == "prod":
        bucket_name = BUCKET_NAME_PROD
        bucket_queries = BUCKET_QUERIES_PROD
    elif env == "dev":
        bucket_name = BUCKET_NAME_DEV
        bucket_queries = BUCKET_QUERIES_DEV
    else:
        sys.exit("INTRODUCE MODE dev OR prod AS --env")
    bucket_obj = s3.Bucket(bucket_name)


    # CONFIGURATION INTERFACES TO VALIDATE
    filename = kwargs["interfaces_file"]
    print("INTERFACES TO VALIDATE")
    file = open(filename, "r")
    reader = csv.reader(file)
    interfaces = []
    for line in reader:
        t=line[0]
        interfaces.append(t)
    print(interfaces)
    # CONFIGURATION PARTITIONS TO VALIDATE
    partitions = kwargs["partitions"]
    if partitions == "all":
        latest = "NO"
    elif partitions == "latest":
        latest = "YES"
    else:
        if len(partitions) != 8:
            sys.exit("INTRODUCE DATE WITH THE CORRECT FORMAT YYYYMMDD")
        else:
            latest = "NO"

    partition = kwargs["unique_partition"]
    if (len(partition) != 0 and len(partition) != 8):
        sys.exit("INTRODUCE partition WITH THE CORRECT FORMAT YYYYMMDD")

    # CONFIGURATION SPARK APPLICATION
    appName = "Validator_external"
    master = "local"
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .config("spark.driver.memory", "8g") \
        .enableHiveSupport() \
        .getOrCreate()


    # VALIDATOR EXTERNAL
    spark.sql("create database if not exists zerebro_gbr")
    #Para cada interface, ejecutar las queries create external y alter table 
    for interface in interfaces:

        dates = []
        prefix='DM/{}/'.format(interface)

        if len(partition) == 0:
            for obj in bucket_obj.objects.filter(Prefix=prefix):
                file = '{0}'.format(obj.key)
                print(file)

                #Busqueda de la fecha en los directorios 
                year_pattern = "dat_exec_year="
                if year_pattern in file:
                    index_year = file.index(year_pattern) + len(year_pattern)
                    month_pattern = "dat_exec_month="
                    index_month = file.index(month_pattern) + len(month_pattern)
                    day_pattern = "dat_exec_day="
                    index_day = file.index(day_pattern) + len(day_pattern)

                    year = file[index_year:index_year + 4]
                    month = file[index_month:index_month + 2]
                    day = file[index_day:index_day + 2]

                    final_date = year + month + day
                    dates.append(final_date)
        else: 
            dates.append(partition)

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
                #En caso de que se quiera coger un intervalo de fechas mas corto, habra que filtrar la lista
                if len(partitions) == 8:
                    print(new_dates)
                    print(partitions)
                    new_dates = list(filter(lambda date: date >= partitions,new_dates))
                    print(new_dates)

            times = len(new_dates)
            
            #Puede que haya varios create_external.sql, por lo tanto habra que recoger todos
            i = 1
            while i != -1: 
                
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
                        error = "Falta Create External"
                        date_gbr = str(datetime.now())
                        resultado = (interface, "*", "No hay CREATE EXTERNAL para esta interface", "KO", error, date_gbr, "*") 
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
                    date_gbr = str(datetime.now())
                    resultado = (interface, "*","CREATE EXTERNAL {}".format(i-1), "OK", "No hay error", date_gbr, "*") 
                    df_resultado.append(resultado)
                    
                    
                except Exception as e:
                    error = str(e)[:24]
                    date_gbr = str(datetime.now())
                    resultado = (interface, "*","CREATE EXTERNAL {}".format(i-1), "KO", error, date_gbr, "*") 
                    df_resultado.append(resultado)
                    break
                
                #Ejecucion del alter table para cada una de las particiones
                provisional_dates = new_dates.copy() #Por si hay varios create external, no borrar la lista de fechas
                for time in range(times):
                    date_part = provisional_dates[-1]
                    del provisional_dates[-1]
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

                        
                        
                        count_records = spark.sql("SELECT count(dat_exec_year) from {}.et_{} where dat_exec_year={} and dat_exec_month={} and dat_exec_day={}".format(schema_prueba,interface,year,month,day)).take(1)
                        #En caso de que encontremos datos en la particion en cuestion de la tabla external
                        count_records_def = (str(count_records[0])).replace("Row(count(dat_exec_year)=","").replace(")","")

                        if int(count_records_def) == 0:
                            print("ALTER FAILED")
                            error = "ALTER fallido"
                            date_gbr = str(datetime.now())
                            resultado = (interface, date_part, "ALTER TABLE", "KO", error, date_gbr, "*")
                            df_resultado.append(resultado)

                        elif (int(count_records_def) > 0 and int(count_records_def) < 3):
                            print("ALTER EXITOSO")
                            error = "Datos no cargados en tabla, fichero vacio"
                            date_gbr = str(datetime.now())
                            resultado = (interface, date_part, "ALTER TABLE", "OK", error, date_gbr, "0")
                            df_resultado.append(resultado)

                        else:
                            error = 'OK'
                            print("ALTER EXITOSO")
                            date_gbr = str(datetime.now())
                            resultado = (interface, date_part, "ALTER TABLE", 'OK', "No hay error", date_gbr, str(int(count_records_def) - 2)) 
                            df_resultado.append(resultado)
                    


                    except Exception as e:
                        error = "ALTER mal implementado o particiones ya introducidas"
                        date_gbr = str(datetime.now())
                        resultado = (interface, date_part, "ALTER TABLE", "KO", error, date_gbr, "*")
                        df_resultado.append(resultado)
                        pass
                
                    # Convert list to RDD
                    rdd = spark.sparkContext.parallelize(df_resultado)

                    # Create data frame
                    df = spark.createDataFrame(rdd,schema)

                    if latest == "NO":
                        df.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_EXTERNAL/report_EXTERNAL_{}_all".format(bucket_queries, date_gbr_run))
                    else:
                        df.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_EXTERNAL/report_EXTERNAL_{}_latest".format(bucket_queries, date_gbr_run))

        except Exception as e:
            error = "Lista de fechas vacia"
            date_gbr = str(datetime.now())
            resultado = (interface, "*", "No hay ficheros de datos en origen para esta interface", "KO", error, date_gbr, "*") 
            df_resultado.append(resultado)
            pass

        # Convert list to RDD
        rdd = spark.sparkContext.parallelize(df_resultado)

        # Create data frame
        df = spark.createDataFrame(rdd,schema)

        if latest == "NO":
            df.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_EXTERNAL/report_EXTERNAL_{}_all".format(bucket_queries, date_gbr_run))
        else:
            df.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_EXTERNAL/report_EXTERNAL_{}_latest".format(bucket_queries, date_gbr_run))
        print(interface + " EJECUTADA CON EXITO")

            
    print("VALIDATOR_EXTERNAL EJECUTADO CON EXITO")

if __name__ == '__main__':

    PARSER = argparse.ArgumentParser(description='GBR TO VERIFY QUERIES')
    PARSER.add_argument('--env', help='set environment to be worked in', default="dev")
    PARSER.add_argument('--interfaces_file', help='set name of the interfaces file that will be processed', default="interfaces.csv")
    PARSER.add_argument('--partitions', help='set the amount of partitions to be worked (all, latest or day until now). Day format: YYYYMMDD', default="latest")
    PARSER.add_argument('--unique_partition', help='set the partition to be worked (YYYYMMDD)', default="")

    main(**vars(PARSER.parse_args()))