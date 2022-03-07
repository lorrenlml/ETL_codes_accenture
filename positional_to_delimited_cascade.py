# %%
import os
import boto3
from argparse import ArgumentParser
from datetime import datetime
from tqdm.auto import tqdm

# %%
parser = ArgumentParser(description="Please provide your Inputs as -i InputFile -o OutPutFile -c ConfigFile")
parser.add_argument("-env", dest="env", required=True,    help="Provide S3 environment (prod or dev)", metavar="STRING")
args = parser.parse_args()
env = args.env

# %%
BUCKET_NAME_DEV = 'bucket-wz-aw-dev-euc-data-ingestion'
BUCKET_NAME_PROD = 'bucket-wz-aw-prod-euc-data-ingestion'
BUCKET_NAME_DEV_REPORT = 'bucket-wz-aw-dev-euc-logs'
BUCKET_NAME_PROD_REPORT = 'bucket-wz-aw-prod-euc-logs'

if env == "prod":
    bucket_name = BUCKET_NAME_PROD
    bucket_name_report = BUCKET_NAME_PROD_REPORT
elif env == "dev":
    bucket_name = BUCKET_NAME_DEV
    bucket_name_report = BUCKET_NAME_DEV_REPORT

#bucket_name = 'bucket-wz-aw-dev-euc-data-ingestion'

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
s3_client = boto3.client('s3')

folder_input = 'positional/'  
folder_output = 'delimited/'
folder_report = 'report/'
folder_schemas = 'interface-schema-field_length/'

# create local folder if not exists

if not os.path.exists(folder_input):
    os.makedirs(folder_input)

if not os.path.exists(folder_output):
    os.makedirs(folder_output)

if not os.path.exists(folder_report):
    os.makedirs(folder_report)

conversion_date = datetime.today().strftime('%Y-%m-%d')

# report output folder in s3
report = []
report_columns = ['Conversion date', 'Input File', 'Interface', 'Status', 'Outcome', 'Solution', 'Responsible']
report_file = '{}{}_delimited_converter_report.csv'.format(folder_report, conversion_date)
folder_report_s3 = 'report_delimited/'
report_file_s3 = '{}{}_delimited_converter_report.csv'.format(folder_report_s3, conversion_date)


files = len(list(bucket.objects.filter(Prefix=folder_input))) -1
print('FILES TO TRANSFORM:', files)

for obj in tqdm(bucket.objects.filter(Prefix=folder_input)):
    key = obj.key
    if key != folder_input: #exclude folder object
        
        # download file from s3 to local
        s3_client.download_file(bucket_name, key, key)
        
        # identify interface
        if 'ODATE' in key:
            interface = key.split('_')[2][7:12] 
        elif 'FDATE' in key:
            interface = key.split('/')[1][7:12]
        else:
            report.append([conversion_date, key.split('/')[1], '', 'KO', 'Interface not identified in file name', 'Review file name', 'WiZink'])
            print('Error: INFERFACE not found in file name {}'.format(key.split('/')[1]))
            break

        # select the schema-field_length.csv file for the interface      
        file_list = os.listdir(folder_schemas)
        for file in file_list:
            if interface in file:
                schema_file = file
                break
        else:
            schema_file = ''

        # delimit the input file if schema-field_length.csv exists
        if schema_file != '':

            # define output delimited file name
            key_output = key.replace(folder_input, folder_output) + '_delimited.txt'
            
            os.system("python ConvertFixedToDelimiter.py -i {key} -o {key_output} -c {schema_file}".format(key = key,
                                                                                                        key_output = key_output, 
                                                                                                        schema_file = folder_schemas + schema_file))
            
            # upload file to s3
            s3_client.upload_file(key_output, bucket_name, key_output)
            report.append([conversion_date, key.split('/')[1], '{}'.format(interface), 'OK', 'Delimited file uploaded to /delimited','',''])

            os.remove(key_output)
        
        else:
            # if no schema file found, add to report
            report.append([conversion_date, key.split('/')[1], '{}'.format(interface), 'KO', 'Schema file not found', 'Review schema files', 'Accenture'])
            print('Error: SCHEMA FILE not found for interface ' + interface)
    
        # delete positional file
        os.remove(key)

# create report file
with open(report_file, 'w') as f:
    f.write('|'.join(report_columns) + '\n')
    for row in report:
        f.write('|'.join(row) + '\n')

s3_client.upload_file(report_file, bucket_name_report, report_file_s3)
print('Report file uploaded to S3 folder:', folder_report_s3)


