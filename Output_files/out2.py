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
MFVector = defaultdict(lambda: {'prod':'', 'month':0, '1_sum_quant':0.0, '2_sum_quant':0.0})
MF_idx=0
MFMap = {}

# Initializing Map for MF_Structure
for i in range(len(records)):
	tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
	if (str(tmprec['prod']) + str(tmprec['month'])) not in MFMap:
		MFVector[MF_idx]['prod']=tmprec['prod']
		MFVector[MF_idx]['month']=tmprec['month']
		MFMap[str(tmprec['prod'])]=MF_idx
		MFMap[str(tmprec['prod']) + str(tmprec['month'])]=MF_idx
		MFMap[str(tmprec['month']) + str(tmprec['prod'])]=MF_idx
		MFMap[str(tmprec['month'])]=MF_idx
		MF_idx+=1 # new idx


# Updating for MF_Structure
for gv in ['1', '2']:
	if gv == '1':
		for i in range(len(records)):
			tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
			index = MFMap[str(tmprec['prod'])+str(tmprec['month'])]
			if ((tmprec['prod']==MFVector[index]['prod']) and (tmprec['month']==MFVector[index]['month']) and (tmprec['year']==2005)):
				MFVector[index]['1_sum_quant']+=tmprec['quant']

	if gv == '2':
		for i in range(len(records)):
			tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
			index = MFMap[str(tmprec['prod'])]
			if ((tmprec['prod']==MFVector[index]['prod'])):
				MFVector[index]['2_sum_quant']+=tmprec['quant']


# Outputing
print('='*100)
print('{:<6}|{:>15}|{:>15}|{:>15}|{:>15}|'.format('index','prod','month','1_sum_quant','2_sum_quant'))
print('='*100)
# Having
output_idx=0
for index in range(len(MFVector)):
	if MFVector[index]['2_sum_quant']!=0:
		output_idx+=1
		print(end='{:<6}|'.format(output_idx))

		print('{:<15}|'.format(MFVector[index]['prod']), end='')
		print('{:>15.0f}|'.format(MFVector[index]['month']), end='')
		print('{:>15.2f}|'.format(MFVector[index]['1_sum_quant']), end='')
		print('{:>15.2f}|'.format(MFVector[index]['2_sum_quant']), end='')
		print('')
