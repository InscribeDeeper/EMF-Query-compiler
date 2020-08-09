# encoding=utf-8

import re

# -----------------------------------------------------------------------------------------------------------#
########################################
### Parsing for Input txt
########################################
def txt_parsing(txt):
    # build the map for input text
    key_place = {}
    keyword = ["(S)", "(n)", "(V)", "([F])", "([C])", "(G)"]
    for i in range(len(keyword)):
        for j in range(i, len(txt)):
            if txt[j].find(keyword[i]) > 0:
                key_place[keyword[i]] = j
    # {'(S)': 0, '(n)': 2, '(V)': 4, '([F])': 6, '([C])': 8, '(G)': 12}
    # print(key_place)
    
    # find the distance with next key word
    params = key_place
    tmp = list(key_place.keys())  # ['(S)', '(n)', '(V)', '([F])', '([σ])', '(G)']
    for i in range(len(key_place) - 1):
        begin = key_place[tmp[i]] + 1  # begin with '(S)': 0+1
        end = key_place[tmp[i + 1]]  # end with '(n)': 2
        #     print(key_place[tmp[i]]+1,key_place[tmp[i+1]])
        params[keyword[i]] = txt[begin:end]  # find the text from begin to end
    # print(txt[key_place[tmp[i+1]]+1:]) # the last one
    params[keyword[i + 1]] = txt[key_place[tmp[i + 1]] + 1:]
    
    # params
    # '''
    # {'(S)': ['cust,1_avg_quant, 2_avg_quant,3_avg_quant'],
    #  '(n)': ['1,2,3'],
    #  '(V)': ['cust'],
    #  '([F])': ['1_sum_quant, 1_avg_quant, 2_avg_quant, 3_sum_quant, 3_avg_quant'],
    #  '([C])': ["1.cust=cust and 1.state='NY' and 1.year=2005", "2.cust=cust and 2.state='CT' and 2.year=2005", "3.cust=cust and 3.state='NJ' and 3.year=2005"],
    #  '(G)': ['1_avg_quant > 2_avg_quant and 1_avg_quant > 3_avg_quant']}'''
    
    # handle several lines
    S = [i.strip() for i in params['(S)'][0].split(',')]
    n = [i.strip() for i in params['(n)'][0].split(',')]
    V = [i.strip() for i in params['(V)'][0].split(',')]
    F = [i.strip() for i in params['([F])'][0].split(',')]
    C = params["([C])"]
    G = [i.strip() for i in params['(G)'][0].split(',')]
    return S, n, V, F, C, G





########################################
### Parsing for G
########################################

def G_parsing(G):
    '''
    input =  ["2_count_prod+2= 1_count_prod+2 and 1_avg_quant<>0"]  # G
    output = "MFVector[index]['2_count_prod']+2==MFVector[index]['1_count_prod']+2
            and MFVector[index]['1_avg_quant']!=0"

    '''
    Having_c = []
    logic_map = {">=": " >= ", "<=": " <= ", "=": " == ", "<>": ' != ', ">": " > ",
                 "<": " < "}  # from left to right, it only execute once. target: insert space for separating
    for one_condition in G[0].split('and'):  # piece = ["2_count_prod+2= 1_count_prod+2" and 1_avg_quant<>0]
        piece = one_condition[:]
        
        # replace once for the logic map
        for key in logic_map.keys():  # order in the logic map matter. if find the '=' first,  the">=" maybe include
            if key in piece:  # subsitute, and break into two part, based on " " whitespace because the map contains the white space in both side
                x = [i.strip() for i in piece.replace(key, logic_map[key]).split(" ") if len(i.strip()) >= 1]
                # print(x)   # x = ['2_count_prod', '==', '1_count_prod+2']
                break  # only replace once
        
        # x = ['2_count_prod', '==', '1_count_prod+2']
        # further replace for +-*/ symbol with re and add the prefix: MFVector[index] etc
        out = compute_str_process(x)
        Having_c.append("".join(out))  # "MFVector[index]['2_count_prod']==MFVector[index]['1_count_prod']+2
    return " and ".join(Having_c)



########################################
# process piece in Condition like
# ['1_sum_quant/2',"==","3"]  ==> ["MFVector[index]['1_sum_quant']/2", '==', '3']
########################################
def compute_str_process(x):

    '''
    process the computational symbol after logic processing

    input1 = ['1_sum_quant/2',"==","3"]
    output1 = ["MFVector[index]['1_sum_quant']/2", '==', '3']

    input2 = ['2_count_prod+2', '==', '1_count_prod+2']
    output2 = ["MFVector[index]['2_count_prod']+2", '==', "MFVector[index]['1_count_prod']+2"]
    '''
    # test
    out = []
    for frac in x:  # three kinds
        
        # if gv in the frac and find and it contains compute symbol  [\+\-\/\*] or other col
        if ("_" in frac) and len(re.findall('(\w+)[\+\-\/\*]', frac)) >= 1:
            col = re.findall('(\w+)[\+\-\/\*]', frac)[0]  # extract the col
            compute_symbol = re.findall('[\+\-\/\*]', frac)
            
            suffix = frac[len(col) + 1::]  # it contains compute symbol  [\+\-\/\*] or other col
            if re.findall('[\D]+', suffix):  # not pure number --> suffix is one col in MFVector
                out.extend(["MFVector[index]['" + col + "']" + compute_symbol[0] + "MFVector[index]['" + suffix + "']"])
            else:  # pure number
                out.extend(["MFVector[index]['" + col + "']" + compute_symbol[0] + suffix])
        elif ("_" in frac):  # if only the gv in the frac
            out.extend(["MFVector[index]['" + frac + "']"])
        else:  # not col in this frac. it means it is "==" or "0" or ">=", etc
            out.append(frac)
    return "".join(out)





########################################
### To be done - input args
########################################
#
# parser = argparse.ArgumentParser()
# parser.add_argument("-i", "--input-file", dest="input_file", type=str, metavar='<str>', default=None)
# parser.add_argument("-o", "--output-file", dest="output_file", type=str, metavar='<str>', default="output_df")
# args = parser.parse_args()
# print(args)
#

# import argparse
# parser = argparse.ArgumentParser()
# parser.add_argument("-i", "--input-file", dest="input_file", type=str, metavar='<str>', default="input.txt")
# parser.add_argument("-o", "--output-file", dest="output_file", type=str, metavar='<str>', default="out1.py")
# # parser.add_argument("-path", "--path", dest="path", type=str, metavar='<str>', default="E:/OneDrive - stevens.edu/Stevens BIA/CS562/Proj/EMF2PY/Input_files/")
# args = parser.parse_args()
# print(args.path)