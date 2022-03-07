# Python program to convert
# JSON file to CSV
import json
import csv

INTERFACE_NAME = 'L22L'

# READ JSON TEMPLATE FOR THE INTERFACE
with open('../copybooks-json/FD' + INTERFACE_NAME + '.TXT.json') as json_file:
	data = json.load(json_file)

len_data = data['transf']

len_data = [[dict['name'],dict['bytes']] for dict in len_data]

# now we will open a file for writing
data_file = open('data_file.csv', 'w')


with open('field_length.csv', 'w', newline='') as f:
      
    # using csv.writer method from CSV package
    write = csv.writer(f)
    write.writerows(len_data)

# create the csv writer object
csv_writer = csv.writer(data_file)
csv_writer.writerow(len_data)
data_file.close()
