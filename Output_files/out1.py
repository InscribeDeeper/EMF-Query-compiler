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
MFVector = defaultdict(lambda: {'cust':'', '1_sum_quant':0.0, '1_count_quant':0, '1_sum_quant':0.0, '1_avg_quant':0.0, '2_count_quant':0, '2_sum_quant':0.0, '2_avg_quant':0.0, '3_sum_quant':0.0, '3_count_quant':0, '3_sum_quant':0.0, '3_avg_quant':0.0, '3_sum_quant':0.0})
MF_idx=0
MFMap = {}

# Initializing Map for MF_Structure
for i in range(len(records)):
	tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
	if (str(tmprec['cust'])) not in MFMap:
		MFVector[MF_idx]['cust']=tmprec['cust']
		MFMap[str(tmprec['cust'])]=MF_idx
		MF_idx+=1 # new idx


# Updating for MF_Structure
for gv in ['1', '2', '3']:
	if gv == '1':
		for i in range(len(records)):
			tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
			index = MFMap[str(tmprec['cust'])]
			if ((tmprec['cust']==MFVector[index]['cust']) and (tmprec['state']=='NY') and (tmprec['year']==2005)):
				MFVector[index]['1_count_quant']+=1
				MFVector[index]['1_sum_quant']+=tmprec['quant']
				MFVector[index]['1_avg_quant']=MFVector[index]['1_sum_quant']/MFVector[index]['1_count_quant']

	if gv == '2':
		for i in range(len(records)):
			tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
			index = MFMap[str(tmprec['cust'])]
			if ((tmprec['cust']==MFVector[index]['cust']) and (tmprec['state']=='CT') and (tmprec['year']==2005)):
				MFVector[index]['2_count_quant']+=1
				MFVector[index]['2_sum_quant']+=tmprec['quant']
				MFVector[index]['2_avg_quant']=MFVector[index]['2_sum_quant']/MFVector[index]['2_count_quant']

	if gv == '3':
		for i in range(len(records)):
			tmprec = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],records[i])}
			index = MFMap[str(tmprec['cust'])]
			if ((tmprec['cust']==MFVector[index]['cust']) and (tmprec['state']=='NJ') and (tmprec['year']==2005)):
				MFVector[index]['3_count_quant']+=1
				MFVector[index]['3_sum_quant']+=tmprec['quant']
				MFVector[index]['3_avg_quant']=MFVector[index]['3_sum_quant']/MFVector[index]['3_count_quant']


# Outputing
print('='*100)
print('{:<6}|{:>15}|{:>15}|{:>15}|{:>15}|{:>15}|'.format('index','cust','1_avg_quant','2_avg_quant','3_avg_quant','3_sum_quant'))
print('='*100)
# Having
output_idx=0
for index in range(len(MFVector)):
	if MFVector[index]['1_avg_quant']>MFVector[index]['2_avg_quant'] and MFVector[index]['1_avg_quant']>=MFVector[index]['3_avg_quant']:
		output_idx+=1
		print(end='{:<6}|'.format(output_idx))

		print('{:<15}|'.format(MFVector[index]['cust']), end='')
		print('{:>15.2f}|'.format(MFVector[index]['1_avg_quant']), end='')
		print('{:>15.2f}|'.format(MFVector[index]['2_avg_quant']), end='')
		print('{:>15.2f}|'.format(MFVector[index]['3_avg_quant']), end='')
		print('{:>15.2f}|'.format(MFVector[index]['3_sum_quant']), end='')
		print('')
