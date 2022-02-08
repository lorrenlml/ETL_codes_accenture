import sys
import boto3
from datetime import date
import re
import csv
s3 = boto3.resource('s3')
client = boto3.client('s3')
#year = str(date.today()).split("-")[1]
insert_date = str(date.today())


#LECTURA INTERFAZ
 
# total arguments
#n = len(sys.argv) - 1
#print("Total arguments passed:", n)

#interfases_list = []
# Arguments passed
#for i in range(n):
#    interfases_list.append(sys.argv[i + 1])


print("\ninterfaces passed:")
#for i in range(n):
#    print(sys.argv[i + 1])

bucket = 'bucket-wz-aw-dev-euc-data-ingestion'
prefix = 'validator/queries_argus/'
result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')

#Creación lista de interfases/directorios
interfases_list = []
for o in result.get('CommonPrefixes'):
    interfases_list.append(o.get('Prefix').replace(prefix,"")[:-1])
    #if interfases_list[-1] == "40506":
        #break
        #interfases_list = []
#interfases_list.remove("10302") 
#interfases_list = ["10310","20382","21062","21880"] #["20263", "20265","20275","20277","20285"]
#print("La lista de directorios a utilizar es la siguiente:")
print(interfases_list)

#print(interfases_list)

#Lista de campos de la external
df_external = []
#Lista de campos encriptados
df_tokenized = []
#Lista de campos de la ods
df_ods = []
#Lista de inserts erroneos
insert_missing = []
#Lista de creates ods erroneos
create_ods_wrong = []
#Lista ddl erroneos
ddl_wrong = []


for interface in interfases_list:
    #Contador ddl e ins
    cont_ddl = 0
    cont_ins = 0
    cont_ods = 0
    cont_ins_2 = 0
    #VARIABLES
    #str(date.today())
    
    schema_external = "zerebro_external"
    schema_ods = "zerebro_ods"
    external_table_name = schema_external + '.et_' + interface
    ods_table_name = schema_ods + '.ods_' + interface
    argus_partition = "partitioned by (`date` string)"
    external_partitition = 'PARTITIONED BY (dat_exec_year string, dat_exec_month string, dat_exec_day string)'
    ods_partition = "PARTITIONED BY (`snapshot` string , `exec_date` string,  `country` string)"
    partition_insert = "PARTITION (snapshot= '${VSNAPSHOT}', exec_date = '${VODATE}' , country ='${VCOUNTRY}') " ################################################################ DEFINIR BIEN (CON ESPACIO AL FINAL)
    create_external_file = "create_external_{}".format(interface)
    create_ods_file = "create_ods_{}".format(interface)
    alter_table_file = "alter_table.sql"
    insert_file = "insert_{}".format(interface)
    msck_external_file = "msck_external_{}.sql".format(interface)
    msck_ods_file = "msck_ods_{}.sql".format(interface)
    report_path = "validator/query_converter_report/"
    

    ods_extra_var = ",insert_date varchar(12)"

    dm_path = 's3a://bucket-wz-aw-dev-euc-external-l0/DM/'
    ods_path = 's3a://bucket-wz-aw-dev-euc-raw-l1/ODS/'

    #KEY QUERIES ARGUS
    prefix = 'validator/queries_argus/{}/'.format(interface)
    #KEY QUERIES MODIFICADAS (ACCENTURE)
    prefix_acc = 'validator/queries/{}/'.format(interface)

    #LECTURA NOMBRES FICHEROS .SQL ARGUS


    ## Bucket to use
    bucket_conn = s3.Bucket(bucket)

    ## List objects within a given prefix
    argus_files = []
    for obj in bucket_conn.objects.filter(Delimiter='/', Prefix=prefix):
        argus_files.append(obj.key.split("/")[3])
        #print(obj.key.split("/")[3])

    ##########################################

    #ACCESO QUERIES ARGUS Y CREACION QUERIES MODIFICADAS
    for file in argus_files:

        full_key = prefix + file
        ods_creates = 0

        #ACCESO QUERIES DDL
        if "_ddl" in file:
            cont_ddl += 1
            if cont_ddl > 1:
                external_table_name = schema_external + '.et_' + interface + "_{}".format(cont_ddl)
            else:
                external_table_name = schema_external + '.et_' + interface
            obj = s3.Object(bucket, full_key)
            initial_query = (obj.get()['Body'].read().decode('utf-8'))
            queries_ddl = initial_query.split(";")
            creates = []

            #DISCARD DROPS AND DISCARD AUDIT_WIZINK
            for i in range(len(queries_ddl)):
                if (("CREATE" in queries_ddl[i]) and ("audit_wizink" not in queries_ddl[i])):
                    creates.append(queries_ddl[i])
                
                #Contemplar caso en el que aparece TERMINATED BY ';' ya que se separaría la query
                if len(queries_ddl[i]) > 0:
                    if queries_ddl[i][0] == "'":
                        creates[-1] = creates[-1] + ";" + queries_ddl[i]

            #Contar creates ods
            ods_creates = len(creates) - 1
            
            #CREATE EXTERNAL
            create_external = creates[0]

            #Comprobacion existencia create external y create ods tables
            if ("_source" not in create_external):
                resultado = (interface, "NO EXTERNAL TABLE en ddl {}".format(cont_ddl))
                ddl_wrong.append(resultado)

            elif len(creates) < 2:
                resultado = (interface, "NO ODS TABLE en ddl {}".format(cont_ddl))
                ddl_wrong.append(resultado)



            #CREATE EXTERNAL_TABLE MODIFICATION

            pattern_table_name = "TABLE"
            pattern_table_name_2 = "("
            index1 = create_external.upper().index(pattern_table_name) + len(pattern_table_name) + 1
            index2 = create_external.index(pattern_table_name_2) #Al no ir incluido el paréntesis no hay que sumar
            create_external = create_external.replace(create_external[index1:index2], external_table_name)
            #Find argus_partition
            pattern_part_argus = "partitioned"
            pattern1 = "("
            pattern2 = ")"

            if pattern_part_argus in create_external.lower():
                
                #Meter particion external que queremos en vez de la que viene
                index0 = create_external.lower().index(pattern_part_argus)
                #open_list = [m.start() for m in re.finditer(pattern1, create_external)]
                close_list = [m.start() for m in re.finditer(re.escape(pattern2), create_external)]
                #open_list.append(index0)
                #open_list.sort
                close_list.append(index0)
                close_list.sort()
                #index_open = open_list.index(index0) + 1
                index_close = close_list.index(index0) + 1
                #print(create_external)
                #print("Este es el indice del PARTITIONED ", index_close)
                #print("Esta es la lista de indices ", close_list)
                index_fin = close_list[index_close]+1
                create_external = create_external.replace(create_external[index0:index_fin],external_partitition)

            else: #Insertamos external_partition si no existe partitioned
                index0 = create_external.index(pattern2)
                create_external = create_external[:index0 + 1] + " " + external_partitition + " " + create_external[index0 + 1:]



            ########
            #try:
            #    index3 = create_external.lower().index(argus_partition)
            #except:
            #    print(interface)
            #index4 = index3 + len(argus_partition)
            #create_external = create_external.replace(create_external[index3:index4],external_partitition)

            #LOCATION
            new_location = "'{}{}/".format(dm_path, interface)
    
            pattern_location = "LOCATION"
            pattern_location_2 = "/'"
            pattern_location_3 = "source'"
            pattern_location_4 = "PARTITIONED"
            #index0 = create_external.upper().index(pattern_location)
            #index1 = create_external.upper().index(pattern_location) + len(pattern_location) + 1
            
            #Busqueda del LOCATION correcto (el que va despues del PARTITIONED BY), ya que puede que haya campos con LOCATION dentro
            location_list = [m.start() for m in re.finditer(pattern_location, create_external.upper())]
            index_part = create_external.upper().index(pattern_location_4)
            location_list.append(index_part)
            location_list.sort()
            index_true_location = location_list.index(index_part) + 1
            true_location = location_list[index_true_location] + len(pattern_location) + 1
         
                
            try:    
                index2 = create_external.index(pattern_location_2) + len(pattern_location_2) - 1
                create_external = create_external.replace(create_external[true_location:index2], new_location)
            except: 
                index3 = create_external.index(pattern_location_3) + len(pattern_location_3) - 1
                create_external = create_external.replace(create_external[true_location:index3], new_location)




            #CREACION QUERIE CREATE_EXTERNAL

            #Teniendo en cuenta si hay varios

            if cont_ddl > 1:
                client.put_object(Body=create_external, Bucket=bucket, Key= prefix_acc + create_external_file + "_{}.sql".format(cont_ddl))
                print("CREATE_EXTERNAL_{} ".format(cont_ddl) + interface + " DONE")

            else:
                client.put_object(Body=create_external, Bucket=bucket, Key= prefix_acc + create_external_file + ".sql")
                print("CREATE_EXTERNAL " + interface + " DONE")
            
    ##########################################

            #OBTENCION CAMPOS DE LA EXTERNAL
            
            first_pattern = "("
            first_pattern2 = "\n"
            #Indices saltos de linea
            indices = [m.start() for m in re.finditer(first_pattern2, create_external)]

            #Comprobación de si hay un campo en la primera linea
            indice1 = create_external.index(first_pattern) + 1
            indices.append(indice1)
            indices.sort()

            #Obtencion posicion del final de 1º linea que tiene el primer campo
            jump_1 = indices[indices.index(indice1) + 1]

            jump_2 = indices[indices.index(indice1) + 2]
            #posible campo en primera linea
            cadena = create_external[indice1:jump_1]

            #En caso de que el primer campo no esté en la primera linea
            #Contemplar que pueda haber espacios
            if (not cadena or cadena.isspace()): 
                
                first_field = create_external[jump_1 + 1:jump_2].split(" ")
                for i in range(len(first_field)):
                    if not (not first_field[i] or first_field[i].isspace()): #Si en la linea en cuestion hay algo que no sean espacios, sera el primer campo (y se coge)
                        first_field_def = first_field[i]
                        resultado = (interface, first_field_def)
                        df_external.append(resultado)
                        break
                
            
                
            #En caso de que el primer campo sí esté en la primera linea
            else:
                first_field_def = create_external[indice1:jump_1].split(" ")[0]
                resultado = (interface, first_field_def)
                df_external.append(resultado)
                #second_field_def = create_external[jump_1 + 1:jump_2]
                #resultado = (interface, second_field_def)
                #df_external.append(resultado)

            
            #Coger el resto de campos menos el último
            fields_pattern = "\n,"
            indices = [m.start() for m in re.finditer(fields_pattern, create_external)]
            for i in range(len(indices) - 1):
                field_ext = create_external[indices[i]+2:indices[i+1]].split(" ")[0]
                resultado = (interface, field_ext)
                df_external.append(resultado)

            if len(indices) > 0: #Por si no hay más campos
                #Coger el último campo
                last_pattern = ")"
                indice2 = create_external.index(last_pattern)
                last_field = create_external[indices[-1]+2:indice2].split(" ")[0]
                resultado = (interface, last_field)
                df_external.append(resultado)




    ##########################################

            #ALTER TABLE
            year = "${VDAT_EXEC_YEAR}"
            month = "${VDAT_EXEC_MONTH}"
            day = "${VDAT_EXEC_DAY}"

            alter_table = '''ALTER TABLE {external_table_name} 
ADD PARTITION (dat_exec_year='{year}',dat_exec_month='{month}',dat_exec_day='{day}') 
LOCATION {new_location}{year}/{month}/{day}' ;'''.format(external_table_name = external_table_name, year = year, month = month, day = day, new_location = new_location)

            #CREACION QUERIE ALTER TABLE

            if cont_ddl > 1:
                alter_table_file = "alter_table_{}.sql".format(cont_ddl)
                client.put_object(Body=alter_table, Bucket=bucket, Key= prefix_acc + alter_table_file)
                print("CREATE_EXTERNAL_{} ".format(cont_ddl) + interface + " DONE")

            else:
                client.put_object(Body=alter_table, Bucket=bucket, Key= prefix_acc + alter_table_file)

            
            print("ALTER TABLE " + interface + " DONE")

    ##########################################
            create_ods_body = ""
            for i in range(ods_creates):

                #CREATE ODS
                create_ods = creates[i + 1]
                

                #CREATE ODS_TABLE MODIFICATION

                pattern_table_name = "TABLE"
                pattern_table_name_2 = "("
                pattern_table_name_3 = "EXTERNAL"
                pattern_table_name_4 = ","
                index1 = create_ods.upper().index(pattern_table_name) + len(pattern_table_name) + 1
                index2 = create_ods.index(pattern_table_name_2) 
                if ods_creates > 1 and i == 0:
                    if cont_ods == 0: #Y solo hay 1 ddl
                        create_ods = create_ods.replace(create_ods[index1:index2], ods_table_name)
                        msck_ods_file = "msck_ods_{}.sql".format(interface)
                        msck = "msck repair table {}".format(ods_table_name)
                    else: #Mas de 1 ddl
                        create_ods = create_ods.replace(create_ods[index1:index2], ods_table_name + "_" + "{}".format(cont_ods+1))
                        msck_ods_file = "msck_ods_{}_{}.sql".format(interface, cont_ods+1)
                        msck = "msck repair table {}".format(ods_table_name + "_" + "{}".format(cont_ods+1))
                elif ods_creates > 1 and i > 0:
                    create_ods = create_ods.replace(create_ods[index1:index2], ods_table_name + "_" + "{}".format(i+1+cont_ods))
                    msck_ods_file = "msck_ods_{}_{}.sql".format(interface, i+1+cont_ods)
                    msck = "msck repair table {}".format(ods_table_name + "_" + "{}".format(i+1+cont_ods))
                else: #Si solo hay 1 create ods
                    if cont_ods == 0: #Y solo hay 1 ddl
                        create_ods = create_ods.replace(create_ods[index1:index2], ods_table_name)
                        msck_ods_file = "msck_ods_{}.sql".format(interface)
                        msck = "msck repair table {}".format(ods_table_name)
                    else: #Mas de 1 ddl
                        create_ods = create_ods.replace(create_ods[index1:index2], ods_table_name + "_" + "{}".format(cont_ods+1))
                        msck_ods_file = "msck_ods_{}_{}.sql".format(interface, cont_ods+1)
                        msck = "msck repair table {}".format(ods_table_name + "_" + "{}".format(cont_ods+1))

                client.put_object(Body=msck, Bucket=bucket, Key= prefix_acc + msck_ods_file)
                print("MSCK " + interface + " DONE")

        

            
                #########################################
                #Find argus_partition
                pattern_part_argus = "partitioned"
                pattern1 = "("
                pattern2 = ")"
                #open_list = [m.start() for m in re.finditer(pattern1, create_external)]
                close_list = [m.start() for m in re.finditer(re.escape(pattern2), create_ods)]
                try:
                    index0 = create_ods.lower().index(pattern_part_argus)
                    #open_list.append(index0)
                    #open_list.sort
                    close_list.append(index0)
                    close_list.sort()
                    #index_open = open_list.index(index0) + 1
                    index_close = close_list.index(index0) + 1
                    index_inic = close_list.index(index0) - 1
                    index_fin = close_list[index_close]+1
                    index_com = close_list[index_inic]
                    create_ods = create_ods.replace(create_ods[index_com:index_fin],ods_extra_var + "\n" + ")" + ods_partition)
                except:
                    pattern2 = "STORED AS"
                    index0 = create_ods.upper().index(pattern2)
                    close_list.append(index0)
                    close_list.sort()
                    index_inic = close_list.index(index0) - 1
                    index_com = close_list[index_inic]
                    index1 = index0 + len(pattern2)
                    create_ods = create_ods.replace(create_ods[index_com:index1], ods_extra_var + "\n" + ")" + ods_partition + "\n" + pattern2)



                ###########################################

                ##Find argus_partition
                #index3 = create_ods.lower().index(argus_partition)
                #index4 = index3 + len(argus_partition)
                #create_ods = create_ods.replace(")" + create_ods[index3:index4],ods_extra_var + "\n" + ")" + ods_partition)
                ##create_ods = create_ods.replace(")" + argus_partition, ods_extra_var + "\n" + ")" + ods_partition)

                create_ods = create_ods.replace(pattern_table_name_3,"",1)
                if "periodid" in create_ods.lower():
                    #Considero que Periodid viene el primero 
                    index3 = create_ods.index(pattern_table_name_2) + len(pattern_table_name_2) 
                    index4 = create_ods.index(pattern_table_name_4) + len(pattern_table_name_4)
                    pattern = "periodid"
                    index_pattern = create_ods.lower().index(pattern)
                    if pattern in create_ods[index3:index4].lower():
                        create_ods = create_ods.replace(create_ods[index3:index4],"")
                    else:
                        pattern_table_name_5 = "\n"
                        jumps = [m.start() for m in re.finditer(re.escape(pattern_table_name_5), create_ods)]

                        jumps.append(index_pattern)
                        jumps.sort()
                        next_jump = jumps[jumps.index(index_pattern) + 1] + 1
                        prev_jump = jumps[jumps.index(index_pattern) - 1] + 1
                        create_ods = create_ods.replace(create_ods[prev_jump:next_jump],"")



                    



                #LOCATION
                new_location = "'{}{}/".format(ods_path, interface)
                

                pattern_location = "LOCATION"
                pattern_location_2 = "/'"
                pattern_location_3 = "target'"
                pattern_location_4 = "PARTITIONED"
                
                #try:
                index1 = create_ods.upper().index(pattern_location) + len(pattern_location) + 1
                #except:
                #    print(interface, create_ods)

                #Busqueda del LOCATION correcto (el que va despues del PARTITIONED BY), ya que puede que haya campos con LOCATION dentro
                location_list = [m.start() for m in re.finditer(pattern_location, create_ods.upper())]
                index_part = create_ods.upper().index(pattern_location_4)
                location_list.append(index_part)
                location_list.sort()
                index_true_location = location_list.index(index_part) + 1
                true_location = location_list[index_true_location] + len(pattern_location) + 1
            
                #if not (create_external[index0 - 1] == "_" or create_external[index1 - 1] == "_"):
                    
                try:    
                    index2 = create_ods.index(pattern_location_2) + len(pattern_location_2) - 1
                    create_ods = create_ods.replace(create_ods[true_location:index2], new_location)
                except: 
                    index3 = create_ods.index(pattern_location_3) + len(pattern_location_3) - 1
                    create_ods = create_ods.replace(create_ods[true_location:index3], new_location)
                
                #Comprobacion sobre la aparicion de STORED AS
                if not ("STORED" in create_ods.upper()):
                    resultado = (interface, "NO STORED AS")
                    create_ods_wrong.append(resultado)


                #OBTENCION CAMPOS Y SUS FORMATOS DE LA TABLA ODS

                #Lista con formatos campos de la ods de cada interface ([(interface, table_name, campo_1, varchar(8)), (interface, table_name, campo_2, decimal(21)])) por ejemplo
                
                #for k in range(len(creates_ods)):

                #PROCESO COGER FORMATO DE CAMPOS ODS (QUE NO SEAN VARCHAR O STRING)

                first_pattern = "("
                first_pattern2 = "\n"
                #Indices saltos de linea
                indices = [m.start() for m in re.finditer(first_pattern2, create_ods)]

                #Comprobación de si hay un campo en la primera linea
                indice1 = create_ods.index(first_pattern) + 1
                indices.append(indice1)
                indices.sort()

                #Obtencion posicion del final de 1º linea que tiene el primer campo
                jump_1 = indices[indices.index(indice1) + 1]

                jump_2 = indices[indices.index(indice1) + 2]
                #posible campo en primera linea
                cadena = create_ods[indice1:jump_1]

                #En caso de que el primer campo no esté en la primera linea
                #Contemplar que pueda haber espacios
                if (not cadena or cadena.isspace()): 
                    #Cojo la primera linea
                    first_field = create_ods[jump_1 + 1:jump_2]
                    
                    #La separo por espacios y creo una lista para poder cogerme el tipo de campo 
                    if "\t" in first_field:
                        first_field = first_field.replace("\n,","")
                        first_field = first_field.replace("\t"," ")
                    first_field_list = list(filter(None,first_field.split(" "))) #Quito los elementos vacios de la lista
                    #print(first_field_list) 
                    #Si la coma viene separada del campo, tendremos que acceder mas adelante a la lista para obtener el tipo
                    if first_field_list[0] == ",":
                        first_field_type = ""
                        first_field_name = first_field_list[1]
                        for f in range(len(first_field_list) - 2):
                            first_field_type += first_field_list[f+2] #+2 ya que el 0 es la "," y el 1 es el nombre del campo
                    else:
                        first_field_type = ""
                        first_field_name = first_field_list[0]
                        for f in range(len(first_field_list) - 1):
                            first_field_type += first_field_list[f+1] #el 0 es el nombre del campo

                    first_field_type = first_field_type.replace(",","") #Por si la coma estaba al final
                    
                    cont = 2



                #En caso de que el primer campo si este en la primera linea
                else:
                    first_field = create_ods[indice1:jump_1]

                    if "\t" in first_field:
                        first_field = first_field.replace("\n,","")
                        first_field = first_field.replace("\t"," ")
                    first_field_list = list(filter(None,first_field.split(" ")))


                    #Si la coma viene separada del campo, tendremos que acceder mas adelante a la lista para obtener el tipo
                    if first_field_list[0] == ",":
                        first_field_type = ""
                        first_field_name = first_field_list[1]
                        for f in range(len(first_field_list) - 2):
                            first_field_type += first_field_list[f+2] #+2 ya que el 0 es la "," y el 1 es el nombre del campo
                    else:
                        first_field_type = ""
                        first_field_name = first_field_list[0]
                        for f in range(len(first_field_list) - 1):
                            first_field_type += first_field_list[f+1] #el 0 es el nombre del campo
                    first_field_type = first_field_type.replace(",","") #Por si la coma estaba al final
                    
                    cont = 1
                
                #Obtencion nombre tabla
                pattern_table_name = "zerebro_ods."
                pattern_table_name_2 = "("
                index_table = create_ods.index(pattern_table_name) + len(pattern_table_name)
                index_fin = create_ods.index(pattern_table_name_2)
                name_table = create_ods[index_table:index_fin]
                #Una vez obtenido el tipo de campo y su nombre se anade a la lista
                resultado = (interface, name_table, first_field_name, first_field_type)
                df_ods.append(resultado)

                while create_ods.index("insert_date") > indices[indices.index(indice1) + cont + 1]:

                    #Coger el resto de tipos 
                    jump_prev = indices[indices.index(indice1) + cont]
                    jump_next = indices[indices.index(indice1) + cont + 1]

                    next_field = create_ods[jump_prev:jump_next]
                    next_field_list = list(filter(None,next_field.split(" ")))

                    out_1 = '\n'
                    out_2 = '\r'
                    out_3 = '\n,'
                    for q in range(len(next_field_list)):
                        next_field_list[q] = next_field_list[q].replace(out_1, "")
                        next_field_list[q] = next_field_list[q].replace(out_2, "")
                        next_field_list[q] = next_field_list[q].replace(out_3, "")
                        if "\t" in next_field_list[q]:
                            next_field = next_field.replace("\n,","")
                            next_field = next_field.replace("\t"," ")
                            next_field_list = list(filter(None,next_field.split(" ")))
                        
                    next_field_list = list(filter(None,next_field_list))
                    if not next_field_list:
                        cont += 1
                    
                    else:
                        #print(next_field_list)
                        #print(next_field_list[0])
                        #Si la coma viene separada del campo, tendremos que acceder mas adelante a la lista para obtener el tipo
                        if next_field_list[0] == ",":
                            next_field_type = ""
                            next_field_name = next_field_list[1]
                            next_field_name = next_field_name.replace(",","")# poner[1:] en caso de que venga con "nombre_campo"
                            for f in range(len(next_field_list) - 2):
                                next_field_type += next_field_list[f+2] #+2 ya que el 0 es la "," y el 1 es el nombre del campo
                        else:
                            next_field_type = ""
                            next_field_name = next_field_list[0]
                            next_field_name = next_field_name.replace(",","")#[1:] #Hacia falta para quitar las comillas del nombre del campo
                            for f in range(len(next_field_list) - 1):
                                next_field_type += next_field_list[f+1] #el 0 es el nombre del campo
                        next_field_type = next_field_type.replace(",","") #Por si la coma estaba al final
                        
                        #Una vez obtenido el tipo de campo y su nombre se anade a la lista
                        resultado = (interface, name_table, next_field_name, next_field_type)
                        df_ods.append(resultado)

                        cont += 1

                        #Con esto tengo una lista de los nombres de los campos y sus tipos para cada interface
                
                create_ods_body += create_ods + ";\n"
            
            cont_ods += ods_creates

            #CREACION QUERIE CREATE_ODS

            #Teniendo en cuenta si hay varios

            if cont_ddl > 1:
                client.put_object(Body=create_ods_body, Bucket=bucket, Key= prefix_acc + create_ods_file + "_{}.sql".format(cont_ddl))
                print("CREATE_ODS_{} ".format(cont_ddl) + interface + " DONE")

            else:
                client.put_object(Body=create_ods_body, Bucket=bucket, Key= prefix_acc + create_ods_file + ".sql")
                print("CREATE_ODS " + interface + " DONE")
            

    ##########################################
    
        #ACCESO QUERIES DML/INS
        elif "_ins" in file:
            cont_ins += 1
            if cont_ins > 1:
                external_table_name = schema_external + '.et_' + interface + "_{}".format(cont_ins)
            else:
                external_table_name = schema_external + '.et_' + interface
            obj = s3.Object(bucket, full_key)
            initial_query = (obj.get()['Body'].read().decode('utf-8'))
            queries_ins = initial_query.split(";")
            inserts = []
            insert_body = ""

            #DISCARD CONFIG AND DISCARD AUDIT_WIZINK
            for querie in queries_ins:
                if (("INSERT" in querie) and ("audit_wizink" not in querie)):
                    inserts.append(querie) 

            for i in range(len(inserts)):
                insert = inserts[i]

                #INSERT_TABLE MODIFICATION
                
                pattern_table_name = "TABLE"
                pattern_table_name_2 = "PARTITION"
                pattern_table_name_3 = "\n,"
                pattern_table_name_4 = ",\n"

                #Cambiar ods_table_name
                if pattern_table_name_2 in insert.upper():
                    index1 = insert.upper().index(pattern_table_name) + len(pattern_table_name) + 1
                    index2 = insert.index(pattern_table_name_2) - 1
                    if len(inserts) > 1 and i == 0:
                        if cont_ins_2 == 0:
                            insert = insert.replace(insert[index1:index2], ods_table_name)
                        else:
                            insert = insert.replace(insert[index1:index2], ods_table_name + "_" + "{}".format(cont_ins_2+i+1))
                    elif len(inserts) > 1 and i > 0:
                        insert = insert.replace(insert[index1:index2], ods_table_name + "_" + "{}".format(cont_ins_2+i+1))
                    else:
                        if cont_ins_2 == 0:
                            insert = insert.replace(insert[index1:index2], ods_table_name)
                        else:
                            insert = insert.replace(insert[index1:index2], ods_table_name + "_" + "{}".format(cont_ins_2+1))


                    #Quitar partition y cast date
                    if pattern_table_name_3 in insert:
                        index3 = insert.index(pattern_table_name_2)
                        index4 = insert.index(pattern_table_name_3) + 2 #Porque realmente quiero quitar la coma también
                        insert = insert.replace(insert[index3:index4], " select \n") 

                    elif pattern_table_name_4 in insert:
                        index3 = insert.index(pattern_table_name_2)
                        index4 = insert.index(pattern_table_name_4) + 2 #Porque realmente quiero quitar la coma también
                        insert = insert.replace(insert[index3:index4], " select \n")  

                else:
                    pattern_table_name_2 = "select"
                    index1 = insert.upper().index(pattern_table_name) + len(pattern_table_name) + 1
                    index2 = insert.lower().index(pattern_table_name_2) - 1
                    if len(inserts) > 1 and i == 0:
                        if cont_ins_2 == 0:
                            insert = insert.replace(insert[index1:index2], ods_table_name)
                        else:
                            insert = insert.replace(insert[index1:index2], ods_table_name + "_" + "{}".format(cont_ins_2+i+1))
                    elif len(inserts) > 1 and i > 0:
                        insert = insert.replace(insert[index1:index2], ods_table_name + "_" + "{}".format(cont_ins_2+i+1))
                    else:
                        if cont_ins_2 == 0:
                            insert = insert.replace(insert[index1:index2], ods_table_name)
                        else:
                            insert = insert.replace(insert[index1:index2], ods_table_name + "_" + "{}".format(cont_ins_2+1))

                ############################################################################################################ METO AQUI EL PARTITION
                 
                indice = insert.index("select")
                insert = insert[:indice] + partition_insert + insert[indice:]
                
                    



                #insert = insert.replace(argus_partition,external_partitition)
                
                #Eliminacion encrypts y obtencion campos encriptados

                while "encrypt(" in insert.lower():
                    #Patron al final de cada linea de campo
                    if "\n," in insert:
                        pattern_encrypt = "\n,"

                    elif ",\n" in insert:
                        pattern_encrypt = ",\n"               
                    
                    pattern_escrypt_2 = "encrypt("
                    
                    #Obtencion posiciones de finalizacion de cada linea
                    indices = [m.start() for m in re.finditer(pattern_encrypt, insert.lower())]
                    
                    indice1 = insert.lower().index(pattern_escrypt_2)
                    indices.append(indice1)
                    indices.sort()
                    
                    #Obtencion posicion del final de 1º linea que tiene encrypt
                    jump = indices[indices.index(indice1) + 1]
                    #Busqueda y eliminacion ultimo caracter línea
                    #El contador (i) ayuda a encontrar los espacios que se quieren borrar
                    i = 1
                    parar = 0
                    #Para borrar los espacios del final de línea (si los hay)
                    while parar == 0: 
                        #Se borra de la insert el espacio del final de la linea en cuestion
                        if insert[jump - i] == ")":
                            insert = insert[0 : jump - i : ] + insert[jump - i + 1 : :]
                            parar = 1
                        #Se actualiza contador para borrar el caracter de una posicion menos
                        else:
                            i += 1
                    #Se borra de la insert el parentesis ")" del final de la linea en cuestion
                    #insert = insert[0 : jump - i : ] + insert[jump - i + 1 : :]
                    
                    indice2 = indice1 + len(pattern_escrypt_2)
                    #Se borra el encrypt
                    insert = insert.replace(insert[indice1:indice2],"",1)
                    #NOTA: se le resta la len() porque se ha borrado esa parte del string

                    #Se obtiene el campo a encriptar 
                    #Añadir campos encriptados a la lista
                    field = insert[indice1:(jump - i - len(pattern_escrypt_2))]
                    #####################################################################################################################
                    

                    #LIMPIEZA PREVIA DE CAMPOS

                    #COMPROBAR APARICION DE "AS" PARA UNA LIMPIEZA MAS RAPIDA
                    #print(field)
                    pattern = " as "
                    if pattern in field.lower():
                        list_as = [m.start() for m in re.finditer(re.escape(pattern), field.lower())]
                        index1 = list_as[-1]
                        index2 = index1 + len(pattern)
                        field = field[index2:]
                        field = field.replace(" ","")
                        field = field.replace(")","")
                        #print(field)



                        #POSIBLES AJUSTES DE CAMPOS
                    adj_1 = "trim("
                    if adj_1 in field.lower():
                        index1 = field.lower().index(adj_1)
                        index2 = index1 + len(adj_1)
                        discard = field[index1:index2]
                        field = field.replace(discard,"")
                        
                    adj_2 = "regexp_replace("
                    if adj_2 in field.lower():
                        index1 = field.lower().index(adj_2)
                        index2 = index1 + len(adj_2)
                        discard = field[index1:index2]
                        field = field.replace(discard,"")

                    adj_3 = "substring("
                    if adj_3 in field.lower():
                        index1 = field.lower().index(adj_3)
                        index2 = index1 + len(adj_3)
                        discard = field[index1:index2]
                        field = field.replace(discard,"")

                    adj_4 = "substr("
                    if adj_4 in field.lower():
                        index1 = field.lower().index(adj_4)
                        index2 = index1 + len(adj_4)
                        discard = field[index1:index2]
                        field = field.replace(discard,"")

                    adj_5 = "replace("
                    if adj_5 in field.lower():
                        index1 = field.lower().index(adj_5)
                        index2 = index1 + len(adj_5)
                        discard = field[index1:index2]
                        field = field.replace(discard,"")

                    adj_6 = "if("
                    if adj_6 in field.lower():
                        index1 = field.lower().index(adj_6)
                        index2 = index1 + len(adj_6)
                        discard = field[index1:index2]
                        field = field.replace(discard,"")       

                    adj_7 = "concat("
                    if adj_7 in field.lower():
                        index1 = field.lower().index(adj_7)
                        index2 = index1 + len(adj_7)
                        discard = field[index1:index2]
                        field = field.replace(discard,"")

                    adj_8 = ")"
                    if adj_8 in field:
                        field = field.replace(adj_8,"")

                        #adj_8 = "case when"
                        #if adj_8 in field.lower():
                        #    index1 = field.lower().index(adj_8)
                        #    index2 = index1 + len(adj_8)
                        #    discard = field[index1:index2]
                        #    field = field.replace(discard,"")

                        #adj_9 = "whole_line"
                        #if adj_9 in field.lower():
                        #    index1 = field.lower().index(adj_9)
                        #    index2 = index1 + len(adj_9)
                        #    discard = field[index1:index2]
                        #    field = field.replace(discard,"")   

                        #adj_10 = "INPUT__FILE__NAME"
                        #if adj_10 in field.lower():
                        #    index1 = field.lower().index(adj_10)
                        #    index2 = index1 + len(adj_10)
                        #    discard = field[index1:index2]
                        #    field = field.replace(discard,"") 

                        #adj_11 = "instr"
                        #if adj_11 in field.lower():
                        #    index1 = field.lower().index(adj_11)
                        #    index2 = index1 + len(adj_11)
                        #    discard = field[index1:index2]
                        #    field = field.replace(discard,"") 

                        #adj_12 = "length"
                        #if adj_12 in field.lower():
                        #    index1 = field.lower().index(adj_12)
                        #    index2 = index1 + len(adj_12)
                        #    discard = field[index1:index2]
                        #    field = field.replace(discard,"")                        
                                        
                        

                        #BUSQUEDA PRIMERA APARICION CAMPO
                        #UPPER CASE ord(char) >= 65 and ord(char) <= 90
                        #LOWER CASE ord(char) >= 97 and ord(char) <= 122 
                        #BARRA BAJA ord(char) == 95
                        #NUMBERS ord(char) >= 48 and ord(char) <= 57

    
                    resultado = (interface, field)
                    df_tokenized.append(resultado)
                    

                #Eliminacion campo 'date' e introducción campos paramétricos

                param_fields = ''','${Vinsert_DATE}' '''

                date_field = ",`date`"

                if date_field not in insert:
                    date_field = "`date`\n"
                    param_fields = ''''${Vinsert_DATE}' '''
                    insert = insert.replace(date_field, param_fields + "\n")
                else:
                    insert = insert.replace(date_field, param_fields)

                if "where " in insert.lower():

                    #Eliminación condición where por defecto
                    date_cond = "`date`"
                    and_cond = " and "
                    index0 = insert.lower().index(date_cond)
                    index_from = insert.lower().index("from ")
                    ands = [m.start() for m in re.finditer(re.escape(and_cond), insert.lower())]
                    ands = [ x for x in ands if x > index_from ]
                    ands.append(index0)
                    ands.sort()
                    condition = ''' dat_exec_year ='${VODATE_YYYY}' 
AND dat_exec_month ='${VODATE_MM}' 
AND dat_exec_day ='${VODATE_DD}' ''' 

                    if len(ands) < 2: #Significara que solo esta la condicion que se quiere eliminar
                
                        index1 = insert.lower().index("where ")
                        insert = insert.replace(insert[index1:],"")
                        condition = ''' \nwhere dat_exec_year ='${VODATE_YYYY}' 
AND dat_exec_month ='${VODATE_MM}' 
AND dat_exec_day ='${VODATE_DD}' ''' 
                        pattern_table_name = "from "
                        index1 = insert.lower().index(pattern_table_name) + len(pattern_table_name) 
                        insert = insert.replace(insert[index1:], external_table_name)
                        insert = insert + condition

                    else: #Si hay varias condiciones, habra que buscar la que se quiere eliminar 

                        #Caso en el que la condicion sobre `date` esta la ultima
                        if ands[-1] == index0:
                            insert = insert.replace(insert[ands[-2]:]," AND " + condition)
                        #Caso en el que la condicion buscada esta la primera
                        elif ands[0] == index0:

                            insert = insert.replace(insert[ands[0]:ands[1]],condition)

                        else:
                            prev_and = ands.index(index0) - 1
                            next_and = ands.index(index0) + 1
                            insert = insert.replace(insert[ands[prev_and]:ands[next_and]]," AND " + condition)
                        
                        index1 = insert.lower().index(" where ")
                        pattern_table_name = "from "
                        index2 = insert.lower().index(pattern_table_name) + len(pattern_table_name) 
                        insert = insert.replace(insert[index2:index1], external_table_name)

                else: #Si no hay where
                    if "from " in insert.lower():
                        pattern_table_name = "from "
                        index1 = insert.lower().index(pattern_table_name) + len(pattern_table_name) 
                        insert = insert.replace(insert[index1:], external_table_name)
                        condition = ''' where dat_exec_year ='${VODATE_YYYY}' 
AND dat_exec_month ='${VODATE_MM}' 
AND dat_exec_day ='${VODATE_DD}' '''
                        insert = insert + condition
                        ################################################################################################### METER WHERE EN CASO DE QUE NO ESTE
                #insert = insert.replace(insert[index1:],"")
                    
                    else:
                        resultado = (interface, "insert_KO")
                        insert_missing.append(resultado)


                insert_body += insert + ";\n"

            cont_ins_2 += len(inserts)
            #CREACION QUERIE INSERT

            #Teniendo en cuenta si hay varios

            if cont_ins > 1:
                client.put_object(Body=insert_body, Bucket=bucket, Key= prefix_acc + insert_file + "_{}.sql".format(cont_ins))
                print("INSERT_{} ".format(cont_ins) + interface + " DONE")

            else:
                client.put_object(Body=insert_body, Bucket=bucket, Key= prefix_acc + insert_file + ".sql")
                print("INSERT " + interface + " DONE")


    ##########################################

    #Creacion msck external
    if cont_ddl > 1:
        external_table_name = schema_external + '.et_' + interface
        msck = "msck repair table {}".format(external_table_name)
        msck_external_file = "msck_external_{}.sql".format(interface)
        client.put_object(Body=msck, Bucket=bucket, Key= prefix_acc + msck_external_file)
        for i in range(cont_ddl - 1):
            external_table_name = schema_external + '.et_' + interface + "_{}".format(i + 2)
            msck = "msck repair table {}".format(external_table_name)
            msck_external_file = "msck_external_{}_{}.sql".format(interface, i + 2)
            client.put_object(Body=msck, Bucket=bucket, Key= prefix_acc + msck_external_file)
    else:
        external_table_name = schema_external + '.et_' + interface
        msck = "msck repair table {}".format(external_table_name)
        msck_external_file = "msck_external_{}.sql".format(interface)
        client.put_object(Body=msck, Bucket=bucket, Key= prefix_acc + msck_external_file)



##CREACION CSV CON CAMPOS DE LA EXTERNAL

filename = 'external_fields.csv'
external_names = ["interface","external_field"]
with open(filename, 'w') as f:

    # using csv.writer method from CSV package
    write = csv.writer(f, delimiter='|')
    
    write.writerow(external_names)
    write.writerows(df_external)


file_obj = open(filename, 'rb')
client.put_object( Bucket=bucket, Key=report_path + filename, Body=file_obj)

##CREACION CSV CON CAMPOS DE LA ODS

filename = 'ods_fields.csv'
ods_names = ["interface", "table_name", "ods_field", "ods_type"]
with open(filename, 'w') as f:

    # using csv.writer method from CSV package
    write = csv.writer(f, delimiter='|')
    
    write.writerow(ods_names)
    write.writerows(df_ods)


file_obj = open(filename, 'rb')
client.put_object( Bucket=bucket, Key=report_path + filename, Body=file_obj)

#CREACION CSV CON CAMPOS TOKENIZADOS

filename = 'encripted_fields.csv'
token_names = ["interface","encripted_field"]
with open(filename, 'w') as g:

    # using csv.writer method from CSV package
    write = csv.writer(g, delimiter='|')
    
    write.writerow(token_names)
    write.writerows(df_tokenized)

file_obj = open(filename, 'rb')
client.put_object( Bucket=bucket, Key=report_path + filename, Body=file_obj)

#CREACION CSV CON INSERTS ERRONEOS
filename = 'insert_wrong.csv'
with open(filename, 'w') as t:

    # using csv.writer method from CSV package
    write = csv.writer(t, delimiter='|')
    
    write.writerow(["interface","status"])
    write.writerows(insert_missing)

file_obj = open(filename, 'rb')
client.put_object( Bucket=bucket, Key=report_path + filename, Body=file_obj)



#CREACION CSV CON CREATE ODS ERRONEOS (no hay STORED)
filename = 'stored_wrong.csv'
with open(filename, 'w') as t:

    # using csv.writer method from CSV package
    write = csv.writer(t, delimiter='|')
    
    write.writerow(["interface","status"])
    write.writerows(create_ods_wrong)

file_obj = open(filename, 'rb')
client.put_object( Bucket=bucket, Key=report_path + filename, Body=file_obj)


#CREACION CSV CON DDL ERRONEOS (no hay create de la external o de la ods)
filename = 'ddl_wrong.csv'
with open(filename, 'w') as t:

    # using csv.writer method from CSV package
    write = csv.writer(t, delimiter='|')
    
    write.writerow(["interface","status"])
    write.writerows(ddl_wrong)

file_obj = open(filename, 'rb')
client.put_object( Bucket=bucket, Key=report_path + filename, Body=file_obj)

#s3.Bucket(bucket).upload_file('/home/hadoop/query_converter/external_fields.csv', report_path)
#s3.Bucket(bucket).upload_file('/home/hadoop/query_converter/encripted_fields.csv', report_path)
#s3.Bucket(bucket).upload_file('/home/hadoop/query_converter/insert_wrong.csv', report_path)