## Códigos para procesos ETL del proyecto Zerebro 2022

# QUERIES

Los scripts 1 y 2 son procesos Python ad-hoc para transformar scripts sql
a un formato adaptado al DWH de destino Hive. Los scripts se modifican una vez se
han ubicado en S3 organizados por carpetas correspondientes a la 
interfaz de ingesta que representan.

Las librerías que se usan son boto3 y pandas.


# VALIDATOR 

Los scripts 3 y 4 son procesos PySpark que validan que los queries ubicadas en los directorios tienen efecto
sobre los ficheros ubicados en los directorios homónimos en otro bucket.
Los directorios de ficheros (raw) tienen además subdirectorios que organizan los ficheros
según su fecha de ingesta, valor que se encuentra en el nombre del fichero. Las 
particiones en origen son por año, mes y día. Las particiones en destino son por 
snapshot, fecha y pais. Este último también extraido del nombre del fichero.

El proceso validator_external genera un reporte con los resultados de ejecución 
de las queries por partición. Consiste en ejecutar la query y, posteriormente, 
comprobar (select) que los datos se han cargado correctamente en la tabla external de Hive.
Si esto no ocurre, es posible que el error sea del propio fichero.

El proceso validator_ods genera un reporte con los resultados de ejecución 
de las queries por partición. Consiste en ejecutar un comando INSERT PARTITION de la tabla
external a la ods para cada partición de la tabla external. 
Para reaprovechar las queries y no tener que modificarlas a SPARK con tipado estricto 
de los campos, se llama a HIVE. 

# comando python
# spark-submit validator.py <interfase>
# nohup spark-submit validator.py <interfase> & //ejecutar en segundo plano