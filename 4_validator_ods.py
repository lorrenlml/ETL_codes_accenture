from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import boto3
import sys
from datetime import datetime
from datetime import date
import os
import csv
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
SCHEMA_PRUEBA = "zerebro_gbr_ods"
SCHEMA_PRUEBA_EXT = "zerebro_gbr"

def main(**kwargs):
    
    # REPORT OUTPUT 
    df_resultado_n = []
    # REPORT SCHEMA
    schema_n = StructType([
        StructField('interface', StringType(), True),
        StructField('partition', StringType(), True),
        StructField('operation', StringType(), True),
        StructField('status', StringType(), True),
        StructField('action_realized', StringType(), True),
        StructField('error_explanation', StringType(), True),
        StructField('date_gbr', StringType(), True),
        StructField('count', StringType(), True)
    ])

    # S3 CONFIGURATION
    client = boto3.client('s3')
    s3 = boto3.resource('s3')
    insert_date = str(date.today())

    env = kwargs["env"]
    if env == "prod":
        bucket_name = BUCKET_NAME_PROD
        bucket_queries = BUCKET_QUERIES_PROD
    elif env == "dev":
        bucket_name = BUCKET_NAME_DEV
        bucket_queries = BUCKET_QUERIES_DEV
    else:
        sys.exit("INTRODUCE MODE --dev OR --prod AS FIRST ARGUMENT")
    bucket_obj = s3.Bucket(bucket_name)


    # CONFIGURATION INTERFACES TO VALIDATE
    filename = kwargs["interfaces_file"]
    if filename == "interfaces.csv":
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
            all = "YES"
        elif partitions == "latest":
            all = "NO"
        else:
            sys.exit("INTRODUCE PATITIONS TO VALIDATE --all OR --latest AS THIRD ARGUMENT")
    else:
        sys.exit("INTRODUCE FILE --interfaces.csv AS SECOND ARGUMENT")

    #CONFIGURATION ASSOCIATED COUNTRIES
    try:
        es_interfaces = kwargs["es_interfaces"].split(',')
        pt_interfaces = kwargs["pt_interfaces"].split(',')
        xx_interfaces = kwargs["xx_interfaces"].split(',')
    except:
        sys.exit("INTRODUCE es_interfaces, pt_interfaces, xx_interfaces AS FOURTH, FIFTH AND SIXTH ARGUMENT")


    # CONFIGURATION SPARK APPLICATION
    appName = "Validator_ods"
    master = "local"
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .enableHiveSupport() \
        .getOrCreate()


    # VALIDATOR ODS
    spark.sql("create database if not exists {}".format(SCHEMA_PRUEBA))

    #Para cada interface, ejecutar las queries create ods e insert
    for interface in interfaces:
        print(interface)

        
        #EJECUCION CREATES ODS
        i = 1
        cont1 = 1
        #Bucle para coger varios archivos de creacion de ods en caso de que los haya
        while i != -1:
            
            if i == 1:
                #Ruta archivos queries ods en caso de que sea el primer create que se coge
                queries_ods = '{queries_path}{interface}/create_ods_{interface}.sql'.format(queries_path = PATH_QUERIES, interface = interface)

            else:
                #Ruta archivos queries ods en caso de que no sea el primer create que se coge
                queries_ods = '{queries_path}{interface}/create_ods_{interface}_{n}.sql'.format(queries_path = PATH_QUERIES, interface = interface, n = i)

            creates_ods = []
            try: #Por si no existe el CREATE, que no pare de ejecutar
                
                
                #Acceso archivo queries_ods
                obj = s3.Object(bucket_queries, queries_ods)

                #Lectura y conversion a string
                initial_query = (obj.get()['Body'].read().decode('utf-8'))

                initial_query = initial_query.replace("zerebro_ods", "if not exists " + SCHEMA_PRUEBA)

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
                    date_gbr = str(datetime.now())
                    resultado_n = (interface, "*","No hay CREATE ODS para esta interface", "KO", "*","*", date_gbr, "*")
                    df_resultado_n.append(resultado_n)
                    break
                else: #Si se esta intentando coger otro create y no hay, no pasa nada
                    break
            
            creates_failed = []
            if len(creates_ods) > 0: #Para recoger todos los creates ods, ya que algunas interfaces tienen varios
                #PROCESO DE LANZAR CREATES
                for k in range(len(creates_ods)):
                    try:
                        spark.sql("""{}""".format(creates_ods[k]))
                        print("CREATE ODS {}".format(cont1) + " hecho")
                        error = "OK"
                        date_gbr = str(datetime.now())
                        resultado_n = (interface, "*","CREATE ODS {}".format(cont1), "OK", "*","*", date_gbr, "*")
                        df_resultado_n.append(resultado_n)

                    except Exception as e:
                        error = str(e)[:24]
                        date_gbr = str(datetime.now())
                        resultado_n = (interface, "*","CREATE ODS {}".format(cont1), "KO", error, "*",date_gbr, "*")
                        df_resultado_n.append(resultado_n)
                        creates_failed.append(k)
                        pass
                    cont1 += 1
                    os.system('''for x in $(yarn application -list -appStates RUNNING | awk 'NR > 2 { print $1 }'); do yarn application -kill $x; done''')


            i += 1 
        
        #EJECUCION INSERTS

        #Busqueda de country en el nombre de los ficheros
        if interface in pt_interfaces:
            country = "PT"
        elif interface in es_interfaces:
            country = "ES"
        else:
            country = "XX"
        
        print('El pais es: ', country)
            

        r = 1
        latest = 1
        cont2 = 1
        while r != -1:
        
            if r == 1:
                #Ruta archivos inserts en caso de que sea el primer insert que se coge
                queries_inserts = '{queries_path}{interface}/insert_{interface}.sql'.format(queries_path = PATH_QUERIES, interface = interface)

            else:
                #Ruta archivos inserts en caso de que no sea el primer insert que se coge
                queries_inserts = '{queries_path}{interface}/insert_{interface}_{n}.sql'.format(queries_path = PATH_QUERIES, interface = interface, n = r)

            inserts = []
            try: #Por si no existe el insert, que no pare de ejecutar
                
                
                #Acceso archivo inserts
                obj = s3.Object(bucket_queries, queries_inserts)

                #Lectura y conversion a string
                initial_query = (obj.get()['Body'].read().decode('utf-8'))

                initial_query = initial_query.replace("zerebro_ods",SCHEMA_PRUEBA)
                initial_query = initial_query.replace("zerebro_external",SCHEMA_PRUEBA_EXT)

                queries = initial_query.split(";")
                

                for j in range(len(queries)):
                    if "INSERT" in queries[j]:
                        inserts.append(queries[j])


            #En caso de no existir el INSERT, guardar el log del error
            except Exception as e:
                if r == 1: #Si se esta cogiendo el primer insert habra que generar un reporte de error
                    error = str(e)
                    date_gbr = str(datetime.now())
                    resultado_n = (interface, "*","No hay INSERT para esta interface", "KO", "*","*", date_gbr, "*")
                    df_resultado_n.append(resultado_n)
                    break
                else: #Si se esta intentando coger otro insert y no hay, no pasa nada
                    break

            if len(inserts) > 0:
                
                try:

                    #PROCESO DE LANZAR INSERTS
                    for k in range(len(inserts)):
                        if k not in creates_failed:

                            new_dates = spark.sql('select (dat_exec_year || dat_exec_month || dat_exec_day) as exec_date from {}.et_{}'.format(SCHEMA_PRUEBA_EXT, interface)). \
                            distinct().rdd.flatMap(lambda x: x).collect()
                            # Caso all == "YES"
                            #Se hara el proceso del insert tantas veces como fechas haya en los ficheros de origen, ya que habra que añadir dichas particiones
                            #Definimos una lista con las fechas existentes sin que esten repetidas, cogiendolas de la tabla external
                            #Ordenamos la lista de menor a mayor para coger la fecha mas reciente primero
                            #Caso all == "NO"
                            if all == "YES":
                                new_dates.sort()
                                print("LA LISTA ORDENADA ES: ", new_dates)
                            else: #all == "NO"
                                max_date = max(new_dates)
                                new_dates = [max_date]
                            times = len(new_dates)
                            #Contador latest para controlar la particion snapshot = 'LATEST' y que las demas sean 'HISTORIC'
                            
                            for time in range(times):
                                date_part = new_dates[-1]
                                #Eliminamos la fecha que hemos cogido para no volverla a coger en la siguiente iteracion
                                del new_dates[-1]

                                print('La fecha maxima en el directorio es: ', date_part)

                                year = date_part[:4]
                                month = date_part[4:6]
                                day = date_part[6:8]

                                count_records_ext = spark.sql("SELECT count(dat_exec_year) from {}.et_{} where dat_exec_year={} and dat_exec_month={} and dat_exec_day={}".format(SCHEMA_PRUEBA_EXT, interface, year,month,day)).take(1)

                                count_records_ext_def = (str(count_records_ext[0])).replace("Row(count(dat_exec_year)=","").replace(")","")  
                                os.system('''for x in $(yarn application -list -appStates RUNNING | awk 'NR > 2 { print $1 }'); do yarn application -kill $x; done''')
                                if int(count_records_ext_def) == 0:
                                    print("INSERT {} {}".format(cont2, date_part) + " EXITOSO")
                                    date_gbr = str(datetime.now())
                                    resultado_n = (interface, date_part, "INSERT {}".format(cont2), "OK", "INSERT NO EJECUTADO", "FICHERO ORIGEN VACÍO", date_gbr, "*")
                                    df_resultado_n.append(resultado_n)

                                else:
                                    #Variable provisional con el insert para luego poder volver a introducir los datos buscados en el insert original, 
                                    #ya que si no tendrian siempre las variables de la primera fecha/particion 
                                    insert_prov = inserts[k]

                                    #Insercion de los campos que nos interesan, ya que en el insert vienen por defecto como variables a introducir
                                    insert_prov = insert_prov.replace("'${Vinsert_DATE}'", "'{}' as insert_date".format(insert_date))
                                    if latest == 1:
                                        insert_prov = insert_prov.replace("'${VSNAPSHOT}'", "'LATEST'")
                                    else:
                                        insert_prov = insert_prov.replace("'${VSNAPSHOT}'", "'HISTORIC'")
                                    insert_prov = insert_prov.replace("'${VODATE}'", "{}".format(date_part))
                                    insert_prov = insert_prov.replace("'${VCOUNTRY}'", "'{}'".format(country))

                                    if "${VODATE_YYYY}" in insert_prov: 
                                        insert_prov = insert_prov.replace('${VODATE_YYYY}', year)
                                        insert_prov = insert_prov.replace('${VODATE_MM}', month)
                                        insert_prov = insert_prov.replace('${VODATE_DD}', day)

                                    #Ejecucion de la querie desde hive, ya que con spark daba problemas el insert
                                    #El problema era debido al cast que hay que hacer a la hora de hacer el insert y que no tenemos
                                    #Este cast lo hace hive por defecto y por eso recurrimos a lanzar la querie desde aqui
                                    #spark.sql("set hive.vectorized.execution.enabled = true")
                                    #spark.sql("set hive.vectorized.execution.reduce.enabled = true")
                                    #spark.sql("set hive.cbo.enable=true;")
                                    #spark.sql("set hive.compute.query.using.stats=true;")
                                    #spark.sql("set hive.stats.fetch.column.stats=true;")
                                    #spark.sql("set hive.stats.fetch.partition.stats=true;")
                                    #-hiveconf hive.execution.engine=tez hive.exec.dynamic.partition=true hive.exec.dynamic.partition.mode=nonstrict;
                                    print(insert_prov)
                                    os.system('hive -e "{}";'.format(insert_prov))
                                    count_records = spark.sql("SELECT count(insert_date) from {}.ods_{} where exec_date = {}".format(SCHEMA_PRUEBA, interface, date_part)).take(1)
                                    #En caso de que encontremos datos en la particion en cuestion de la tabla external
                                    count_records_def = (str(count_records[0])).replace("Row(count(insert_date)=","").replace(")","")


                                    if int(count_records_def) == 0:
                                        print("INSERT {} {}".format(cont2, date_part) + " FALLIDO")
                                        date_gbr = str(datetime.now())
                                        resultado_n = (interface, date_part, "INSERT {}".format(cont2), "KO", "INSERT EJECUTADO", "SELECT NO DEVUELVE NADA INSERT ERRONEO", date_gbr, "*")
                                        df_resultado_n.append(resultado_n)

                                    else:

                                        print("INSERT {} {}".format(cont2, date_part) + " EXITOSO")
                                        date_gbr = str(datetime.now())
                                        resultado_n = (interface, date_part, "INSERT {}".format(cont2), "OK", "INSERT EJECUTADO", "SELECT DEVUELVE PARTICION CON DATOS", date_gbr,str(int(count_records_def)))
                                        df_resultado_n.append(resultado_n)
                                        latest += 1

                                    os.system('''for x in $(yarn application -list -appStates RUNNING | awk 'NR > 2 { print $1 }'); do yarn application -kill $x; done''')

                                # Convert list to RDD
                                rdd_n = spark.sparkContext.parallelize(df_resultado_n)

                                # Create data frame
                                df_n = spark.createDataFrame(rdd_n,schema_n)

                                if all == "YES":
                                    df_n.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_ODS/report_ODS_{}_all".format(bucket_queries, insert_date))
                                else:
                                    df_n.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_ODS/report_ODS_{}_latest".format(bucket_queries, insert_date))
                        else:
                            date_gbr = str(datetime.now())
                            resultado_n = (interface, "*", "INSERT {}".format(cont2), "KO", "INSERT NO EJECUTADO", "CREATE ODS ASOCIADO FALLIDO", date_gbr, "*")
                            df_resultado_n.append(resultado_n)
                            # Convert list to RDD
                            rdd_n = spark.sparkContext.parallelize(df_resultado_n)

                            # Create data frame
                            df_n = spark.createDataFrame(rdd_n,schema_n)
                            if all == "YES":
                                df_n.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_ODS/report_ODS_{}_all".format(bucket_queries, insert_date))
                            else:
                                df_n.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_ODS/report_ODS_{}_latest".format(bucket_queries, insert_date))
                        cont2 += 1
                except Exception as e:
                    error = str(e)[:46]
                    print(error)
                    date_gbr = str(datetime.now())
                    resultado_n = (interface, date_part,"INSERT", "KO", "INSERT NO EJECUTADO EN HIVE", error, date_gbr, "*")
                    df_resultado_n.append(resultado_n)
                    # Convert list to RDD
                    rdd_n = spark.sparkContext.parallelize(df_resultado_n)

                    # Create data frame
                    df_n = spark.createDataFrame(rdd_n,schema_n)
                    if all == "YES":
                        df_n.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_ODS/report_ODS_{}_all".format(bucket_queries, insert_date))
                    else:
                        df_n.coalesce(1).write.option("header", True).mode("overwrite").option("delimiter","|").format("csv").save("s3://{}/validator/report_ODS/report_ODS_{}_latest".format(bucket_queries, insert_date))
                    
                    pass
            r += 1

            
    print("VALIDATOR_ODS EJECUTADO CON EXITO")

if __name__ == '__main__':

    PARSER = argparse.ArgumentParser(description='GBR TO VERIFY QUERIES')
    PARSER.add_argument('--env', help='set environment to be worked in', default="dev")
    PARSER.add_argument('--interfaces_file', help='set name of the interfaces file that will be processed', default="interfaces.csv")
    PARSER.add_argument('--partitions', help='set the amount of partitions to be worked (all or latest)', default="latest")
    PARSER.add_argument('--es_interfaces', help='set list of ES interfaces as string ("" for empty list)')
    PARSER.add_argument('--pt_interfaces', help='set list of PT interfaces as string ("" for empty list)')
    PARSER.add_argument('--xx_interfaces', help='set list of XX interfaces as string ("" for empty list)')
    main(**vars(PARSER.parse_args()))
