#!/usr/bin/env python
# coding: utf-8
import psycopg2
from emf2py import emf_to_py
from utils import *
import argparse
import sys
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--code-path", dest="code_path", type=str, metavar='<str>', default="E:/OneDrive - stevens.edu/Stevens BIA/CS562/Proj/EMF2PY/")
parser.add_argument("-i", "--input-file", dest="input_file", type=str, metavar='<str>', default="Input_files/input1.txt")
parser.add_argument("-o", "--output-file", dest="output_file", type=str, metavar='<str>', default="Output_files/out1.py")
args = parser.parse_args()
# print(args.input_file)
# print(args.output_file)

# input_path = "E:/OneDrive - stevens.edu/Stevens BIA/CS562/Proj/EMF2PY/" + "Input_files/"
# output_path = "E:/OneDrive - stevens.edu/Stevens BIA/CS562/Proj/EMF2PY/" + "Output_files/"
with open(args.code_path + args.input_file, 'r', encoding='utf') as f:
	txt = f.read().strip().split('\n')


########################################
# connect DB login  ==> get schema & log info
########################################
table = "sales"
dbname = "postgres"
host = "localhost"
user = "postgres"
password = "00000000"
port = "5388"

try:
	connection = psycopg2.connect(user=user, password=password, host=host, port=port, database=dbname)
	query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + table + "';"
	cursor = connection.cursor()
	cursor.execute(query)
	if cursor.rowcount == 0:
		print("Connected, but no data in table: %s " % table)
		sys.exit(0)
	schema = cursor.fetchall()
except(Exception, psycopg2.Error) as error:
	print("Error connecting to PostgreSQL database ==>", error)
	sys.exit(0)

print("=" * 40)
print("In <<'%s'>> table" % format(table))
print("=" * 40)
print("ATTRIBUTES".ljust(14, " "), "DTYPE".ljust(15, " "))
for i in schema:
	print("*", i[0].ljust(12, " "), i[1].ljust(15, " "))
print("=" * 40)


PG_log_info = {'user': user, 'password': password, 'host': host, 'port': port, 'database': dbname}



########################################
# Parsing input txt
########################################
S, n, V, F, C, G = txt_parsing(txt)


########################################
# Processing and Save result
########################################
res = emf_to_py(PG_log_info, S,n,V,F,C,G, schema)



to_file = args.code_path + args.output_file
with open(to_file, "w", encoding='utf-8') as f:
    f.write(res)
    print("The file has been saved in Output_files:", './Output_files/'+args.output_file)
