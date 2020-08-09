from itertools import combinations_with_replacement as cmb
from utils import *

def emf_to_py(PG_log_info, S,n,V,F,C,G, schema):
    ########################################
    ### initialization
    ########################################
    # T_cols = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant']
    T_cols = [i[0] for i in schema]
    
    ## 3个mapping
    sql_dtypes_maps = {"character varying": "''", "character": "''", "integer": 0}
    logic_map = {">=": " >= ", "<=": " <= ", "=": " == ", "<>": ' != ', ">": " > ",
                 "<": " < "}  # for insert space ans subtitute logic symbol in order. Only execute once in loop
    format_length_map = {"''": "{:<12}", 0: "{:>15}", 0.0: "{:>15}"}
    
    V_cols = [i.strip() for i in V]  # grouping variable = ['cust', 'prod']

    # MF_dtypes = {'cust': "''", 'prod': "''", 'day': 0, 'month': 0, 'year': 0, 'state': "''", 'quant': 0, '_1_avg_quant': 0.0, '_1_count_quant': 0, '_1_sum_quant': 0.0, '_1_max_quant': 0.0, '_1_min_quant': 0.0, '_2_avg_quant': 0.0, '_2_count_quant': 0, '_2_sum_quant': 0.0, '_2_max_quant': 0.0, '_2_min_quant': 0.0}
    # MF_cols = ['prod', 'month', '_1_avg_quant', '_1_count_quant', '_1_sum_quant', '_1_max_quant', '_1_min_quant', '_2_avg_quant', '_2_count_quant', '_2_sum_quant', '_2_max_quant', '_2_min_quant'... month etc ]
    MF_cols = V[:] # Grouping variables
    for agg_col in F[:]: # for agg function. build MF_Vector
        tmp_mf = []
        if "avg" in agg_col:
            tmp_mf.append(agg_col.replace('avg', 'count'))
            tmp_mf.append(agg_col.replace('avg', 'sum'))
            tmp_mf.append(agg_col.replace('avg', 'avg'))
        else:
            tmp_mf = [agg_col]
        MF_cols.extend(tmp_mf)



    ########## 这里不需要
    MF_dtypes = {i[0]: sql_dtypes_maps[i[1]] for i in schema}
    for i in schema:  # ('prod', 'character varying'),...]
        if type(sql_dtypes_maps[i[1]]) == str:  # if it is string
            for gv in n:
                Agg_F = ["{}_count_{}".format(gv, i[0])]
                #             MF_cols.extend(Agg_F)
                MF_dtypes.update({i: j for i, j in zip(Agg_F, [0])})
        else:  # int
            for gv in n:
                Agg_F = (
                    "{}_avg_{},{}_count_{},{}_sum_{},{}_max_{},{}_min_{}".format(gv, i[0], gv, i[0], gv, i[0], gv, i[0],
                                                                                 gv, i[0])).split(',')
                #             MF_cols.extend(Agg_F) # There only focus on quant. if other than quant, there should be more
                MF_dtypes.update({i: j for i, j in zip(Agg_F, [0.0, 0, 0.0, 0.0, 0.0])})
    ########## 这里不需要
                
    C_cols = [row.strip().split("and") for row in C if len(row) > 2]
    # ['1.prod=prod and 1.month=month and 1.year=1997', '2.prod=prod and 2.year=1997']
    
    
    # In[31]:
    
    
    ########################################
    ### Connection_initialize
    ########################################
    import_text = 'import psycopg2\nimport sys\nfrom collections import defaultdict'
    initial_list = list(PG_log_info.keys())
    connection_text = '\ntry:\n    connection = psycopg2.connect('
    for i in initial_list:
        connection_text += str(i) + "='" + str(PG_log_info[i]) + "', "
    connection_text += ''')\n    query = "SELECT * FROM sales"\n    cursor = connection.cursor()\n    cursor.execute(query)\n    if cursor.rowcount == 0:\n        print("Connected, but no data!")\n        sys.exit(0)
                        \n    records = cursor.fetchall()\nexcept(Exception, psycopg2.Error) as error:\n    print("Error connecting to PostgreSQL database ==>", error)\n    sys.exit(0)\n
                        '''
    
    
    
    
    ########################################
    # Initializing MF_Vector
    ########################################
    MFVector_text = "MFVector = defaultdict(lambda: {"
    for col_name in MF_cols:
        MFVector_text += "'" + col_name + "'" + ':' + str(MF_dtypes[col_name]) + ', '
    MFVector_text = MFVector_text[:-2] + '})' + '\nMF_idx=0\nMFMap = {}\n'
    
    # In[13]:

    ########################################
    # Initializing Map for MF_Structure
    ########################################
    scan_text = "for i in range(len(records)):\n\ttmprec = {x[0]:x[1] for x in zip(" + str(T_cols) + ",records[i])}\n"

    MFMap_initing = []
    MFMap_initing.append("\tif (" + " + ".join(['str(tmprec' + str([i]) + ")" for i in V_cols]) + ") not in MFMap:\n")
    MFMap_initing.append(
        "\t\t" + "\n\t\t".join(['MFVector[MF_idx]' + str([i]) + '=tmprec' + str([i]) for i in V_cols]) + '\n')

    for idx_combination in cmb(V_cols,
                               len(V_cols)):  # ['cust', 'prod] ==>>> ['cust','cust],['prod','prod'], ['cust','prod']
        t = '\t\tMFMap['
        if len(set(idx_combination)) > 1:  # if they are not the same
            t = '\t\tMFMap['
            t += " + ".join(['str(tmprec' + str([j]) + ")" for j in
                             list(idx_combination)])  # MFMap[tmprec['prod'] + tmprec['month']]=MF_idx
            t += ']=MF_idx\n'
        
            ######################################## Correction on May 8
            t += '\t\tMFMap['
            t += " + ".join(['str(tmprec' + str([j]) + ")" for j in list(idx_combination)[::-1]])
            t += ']=MF_idx\n'
            #########################################
        else:  # if they are the same
            t += 'str(tmprec' + str(list(set(idx_combination))) + ")"  # MFMap[tmprec['prod']]=MF_idx
            t += ']=MF_idx\n'
        MFMap_initing.append(t)

    MFMap_text = scan_text + "".join(MFMap_initing) + "\t\tMF_idx+=1 # new idx\n\n"  # MFMap_initial_text

    # In[14]:

    ########################################
    ## # Updating for MF_Structure  -  V1
    ########################################
    MF_update = "for gv in " + str(n) + ":\n"
    scan_text_2 = "\t\tfor i in range(len(records)):\n\t\t\ttmprec = {x[0]:x[1] for x in zip(" + str(
        T_cols) + ",records[i])}\n"
    for one_gv in range(len(n)):
        Map_index = '\t\t\tindex = MFMap['
        indexing_cols = []
        Condition = '\t\t\tif ('
        conditioning_cols = []
        Update = ''
        updating_cols = []
        for piece in C_cols[
            one_gv]:  # C_cols = [['1.prod=prod ', ' 1.month=month ', ' 1.year=1997'], ['2.prod=prod ', ' 2.year=1997']]
        
            ############# indexing_cols  E.g. 1.cust=cust
            #### output ####  index = MFMap[tmprec['prod']+tmprec['month']]
            if ('=' in piece) and (">=" not in piece) and ("<=" not in piece):  # find the text after "="
                t = piece.split("=")[
                    -1].strip()  # find the indexing cols which exist "=" symbol. E.g. 1.cust=cust , the later 'cust'
                if t in V_cols:  # intersect with MF_Vector combined index
                    # print(V_cols)
                    indexing_cols.append("str(tmprec['" + t + "'])")  # This have to be string
                    t = ''
        
            ############ Condition  # subsitute the condition symbol in the map
            #### output #### if ((tmprec['prod']==MFVector[index]['prod']) and (tmprec['month']==MFVector[index]['month']) and (tmprec['year']==1997)):
            for key in logic_map.keys():  # order in the logic map matter. if find the '=' first,  the">=" maybe include
                if key in piece:  # subsitute, and break into two part, based on " " whitespace because the map contains the white space in both side
                    #                 x = piece.replace(key, logic_map[key]).strip().split(" ")
                    x = [i.strip() for i in piece.replace(key, logic_map[key]).split(" ") if
                         len(i.strip()) >= 1]  # '2.prod=prod' ==> [2.prod, ==,  prod]
                    break  # only replace once
        
            if x[2] in V_cols:  # the later one not exist in V_cols for MF_V indexing
                conditioning_cols.append(
                    "(tmprec['" + x[0][(x[0].find('.') + 1):] + "']" + x[1] + "MFVector[index][" + "'" + x[
                        2] + "'])")  # x =  [2.prod, ==,  prod]
            else:
                conditioning_cols.append(
                    "(tmprec['" + x[0][(x[0].find('.') + 1):] + "']" + x[1] + x[2] + ")")  # E.g. x = [3.year, ==, 1997]
    
        ############ Update loop for each gv
        agg_cols = [col for col in MF_cols if (n[one_gv]) == col[
            0]]  # find the related gv cols in F. at the first begining one . e.g. gv = first of '2_avg_quant'
        #     print(agg_cols)
    
        ######## key code: for handle one gv for other attrs
        ##### e.g. for [x_count_prod,  x_avg_quant, x_sum_quant]
        ##### get gv_attrs_set = [prod, quant]
        ########
        gv_attrs_set = list(set([gv_attrs.split('_')[2] for gv_attrs in agg_cols]))  # get gv_attrs_set = [prod, quant]
        for attrs in gv_attrs_set:  # for gv=x, for attrs in [prod, quant] ...
            agg_attr_cols = [each for each in agg_cols if
                             each.find(attrs) > 1]  # 找到该 gv 下面 只包含  prod 的所有 agg_cols =  x_count_prod
        
            updating_col = []  # one gv, one attrs for it
            for agg_col in agg_attr_cols:
                y = agg_col.split('_')  # ['2', 'avg', 'quant'] # meet avg , break
                # identify agg function
                if 'avg' == y[-2]:
                    updating_col = []  # meet 'avg', append with sum and count
                    updating_col.append("\t\t\t\tMFVector[index]['" + agg_col.replace('avg', 'count') + "']+=1\n")
                    updating_col.append(
                        "\t\t\t\tMFVector[index]['" + agg_col.replace('avg', 'sum') + "']+=" + "tmprec['" + y[
                            -1] + "']\n")
                    updating_col.append(
                        "\t\t\t\tMFVector[index]['" + agg_col + "']=" + "MFVector[index]['" + agg_col.replace('avg',
                                                                                                              'sum') + "']" + '/' + "MFVector[index]['" + agg_col.replace(
                            'avg', 'count') + "']\n")
                    break
                elif ('sum' == y[-2]):
                    updating_col.append(
                        "\t\t\t\tMFVector[index]['" + agg_col.replace('avg', 'sum') + "']+=" + "tmprec['" + y[
                            -1] + "']\n")
                elif ('count' == y[-2]):
                    updating_col.append("\t\t\t\tMFVector[index]['" + agg_col.replace('avg', 'count') + "']+=1\n")
                else:
                    print(":< This poor codes cannot handle this aggregation function:", y[-2])
            #             print(updating_col) # one gv, one attrs for it
            updating_cols.extend(updating_col)  # one gv, every attrs for it!!
        #         print(updating_cols)  # one gv, every attrs for it
    
        gv_judge = "\tif gv == " + str("'" + n[one_gv] + "'") + ":\n"
        Map_index += "+".join(indexing_cols) + "]\n"  # index = MFMap[str(tmprec['cust'])+str(tmprec['prod'])]
        Condition += " and ".join(
            conditioning_cols) + "):\n"  # if ((tmprec['cust']==MFVector[index]['cust']) and (tmprec['prod']==MFVector[index]['prod'])):
        Update += "".join(updating_cols) + "\n"  # MFVector[index]['2_count_quant']+=1
        # print(Map_index,Condition, Update)
        one_gv_scan_text = gv_judge + scan_text_2 + Map_index + Condition + Update
    
        MF_update += one_gv_scan_text
        
    # In[22]:
    
    
    ########################################
    # Having
    ########################################
    Having_text = 'output_idx=0\n'
    Having_text += "for index in range(len(MFVector)):\n"
    Having_text += "\tif " + G_parsing(G) + ":\n"
    Having_text += '\t\toutput_idx+=1\n'  # for output index
    Having_text += "\t\tprint(end='{:<6}|'.format(output_idx))\n"

    ########################################
    # Output
    ########################################
    ### Table_cols_name_output
    Table_cols_name_output = "print('='*100)\n"
    format_ = "print('{:<6}|"
    cols = "'.format('index'"
    
    for col in S:
        try:  # detect exist or not in MF_dtypes
            format_ += format_length_map[MF_dtypes[i]] + '|'
            cols += ",'" + col + "'"
        except:  # if not , it is float, and the selection part need change
            format_ += format_length_map[0] + '|'
            cols += ",'" + col + "'"
    Table_cols_name_output += format_ + cols + '))\n' + "print('='*100)\n"
    
    Selection_text = ''
    for col in S:
        try:  # simple
            if type(MF_dtypes[col]) == str:
                Selection_text += "\t\tprint('{:<15}|'.format(MFVector[index]['" + col + "']), end='')\n"
            elif (type(MF_dtypes[col]) == int):
                Selection_text += "\t\tprint('{:>15.0f}|'.format(MFVector[index]['" + col + "']), end='')\n"
            elif (type(MF_dtypes[col]) == float):
                Selection_text += "\t\tprint('{:>15.2f}|'.format(MFVector[index]['" + col + "']), end='')\n"
        except:  # the combined output
            Selection_text += "\t\tprint('{:>15.2f}|'.format(" + compute_str_process(
                [col]) + "), end='')\n"  # it can only process list
    
    Selection_text += "\t\tprint('')"
    
    # In[18]:
    
    
    # ########################################
    # # Save result
    # ########################################
    # test = 2
    # file_name = "./out_%s.py" % test
    #
    # with open(file_name, "w", encoding='utf-8') as f:
    res = '# Import and Connecting DB\n'
    res += import_text + '\n'
    res += connection_text + '\n'
    res += '# Initializing MF_Vector\n'
    res += MFVector_text + '\n'
    res += '# Initializing Map for MF_Structure\n'
    res += MFMap_text + '\n'
    res += '# Updating for MF_Structure\n'
    res += MF_update + '\n'
    res += '# Outputing\n'
    res += Table_cols_name_output
    res += '# Having\n'
    res += Having_text + '\n'
    res += Selection_text + '\n'
    #
    #     f.write(res)
    #     print("The file has been saved")
        
    return res