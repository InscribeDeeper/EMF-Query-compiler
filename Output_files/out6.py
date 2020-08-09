# Import and Connecting DB
import psycopg2
import sys
from collections import defaultdict

try:
    connection = psycopg2.connect(user='postgres', password='00000000', host='localhost', port='5388', database='postgres', )
    query = "SELECT * FROM sales"
    cursor = connection.cursor()
    cursor.execute(query)
    if cursor.rowcount == 0:
        print("Connected, but no data!")
        sys.exit(0)
                        
    records = cursor.fetchall()
except(Exception, psycopg2.Error) as error:
    print("Error connecting to PostgreSQL database ==>", error)
    sys.exit(0)

                        
# Initializing MF_Vector
MFVector = defaultdict(lambda: {'prod':'', 'quant':0, '1_count_prod':0, '2_count_prod':0})
MF_idx=0
MFMap = {}

# Initializing Map for MF_Structure
for i in range(len(records)):
	tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
	if (str(tmprec['prod']) + str(tmprec['quant'])) not in MFMap:
		MFVector[MF_idx]['prod']=tmprec['prod']
		MFVector[MF_idx]['quant']=tmprec['quant']
		MFMap[str(tmprec['prod'])]=MF_idx
		MFMap[str(tmprec['prod']) + str(tmprec['quant'])]=MF_idx
		MFMap[str(tmprec['quant']) + str(tmprec['prod'])]=MF_idx
		MFMap[str(tmprec['quant'])]=MF_idx
		MF_idx+=1 # new idx


# Updating for MF_Structure
for gv in ['1', '2']:
	if gv == '1':
		for i in range(len(records)):
			tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
			index = MFMap[str(tmprec['prod'])]
			if ((tmprec['prod']==MFVector[index]['prod'])):
				MFVector[index]['1_count_prod']+=1

	if gv == '2':
		for i in range(len(records)):
			tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
			index = MFMap[str(tmprec['prod'])]
			if ((tmprec['prod']==MFVector[index]['prod']) and (tmprec['quant']<MFVector[index]['quant'])):
				MFVector[index]['2_count_prod']+=1


# Outputing
print('='*100)
print('{:<6}|{:>15}|{:>15}|'.format('index','prod','quant'))
print('='*100)
# Having
output_idx=0
for index in range(len(MFVector)):
	if MFVector[index]['2_count_prod']==MFVector[index]['1_count_prod']/2:
		output_idx+=1
		print(end='{:<6}|'.format(output_idx))

		print('{:<15}|'.format(MFVector[index]['prod']), end='')
		print('{:>15.0f}|'.format(MFVector[index]['quant']), end='')
		print('')
