import sys
import re
import csv
import sqlparse
import operator
from prettytable import PrettyTable
from operator import itemgetter
identifiers=[]
selectcolumns=[]
tablesjoin=[]
columnsjoin=[]
joinvalues=[]
aggregatefunction = []
aggregatecolumns = []
tables={}
aggregatefunction = []
aggregatecolumn = []
operators = ['<=','>=','<','>','=']
csvdata={}
groupbydata = {}
result = []
ans = []
def readtxt ():
    with open("metadata.txt","r") as file:
        tablename=""
        tableattributes=[]
        begintable = False
        count = 0
        for line in file:
            line = line.replace('\n','')
            endtable = False
            if(line[0] == '<' and line[1]=='b'):
                begintable = True
                count = 1
            elif(line[0] == '<' and line[1] == 'e'):
                begintable = False
                tables[tablename]=tableattributes
                tablename = ""
                tableattributes = []
            elif(begintable == True and count == 1):
                tablename = line
                tables[tablename] = tableattributes
                csvdata[tablename]={}
                count = 0
            elif(begintable == True and count == 0):
                tableattributes.append(line)
                csvdata[tablename][line] = []   
def csvparse():
    for tot in csvdata.keys():
        data=[]
        with open('./'+str(tot)+'.csv')as f:
            for line in f:
                line = line.strip("\n")
                data = [int(value.strip("\'\"")) for value in line.split(',') if not value == '']
                columns = []
                columns = tables[tot]
                i = 0
                while(i<len(columns)):
                    csvdata[tot][columns[i]].append(data[i])
                    i = i+1     
def parsequery(inputsql,identifiers, result,ans,tables,joinvalues):
    dist = False
    if(not(inputsql[-1] == ';')):
        print("Error, put ; at the end")
        return
    inputsql = inputsql.split(';')[0]    
    inputsql = sqlparse.format(inputsql,keyword_case='upper')
    parsing = sqlparse.parse(inputsql)
    parsingg = parsing[0]
    tokenlist = sqlparse.sql.IdentifierList(parsingg.tokens).get_identifiers()
    for val in tokenlist:
        identifiers.append(str(val))
    if len(identifiers) < 4:
        print("Err: Expected command like SELECT __ FROM __;")
        return
    if not identifiers[0].upper() == 'SELECT':
        print("Err: Expected SELECT at the begining")
        return
    if not (identifiers[3].upper() == 'FROM' or identifiers[2].upper() == 'FROM'):
        print("Err: Expected a FROM clause or of form SELECT DISTINCT __ FROM __;")
        return 
    for i in identifiers:
        if(i == "DISTINCT"):
            dist = True
            break 
    stat = 0
    if(identifiers[1] == "*" or identifiers[2] == "*"):
        stat = 1 
    if(dist == True):
        selectcolumns = identifiers[2].replace(" ","").split(",")      
    else :
        selectcolumns = identifiers[1].replace(" ","").split(",")    
    crossjoin(identifiers,joinvalues)
    if(len(joinvalues) == 0):
        print("Wrong Query")
        sys.exit(0)
    flag = whereclause(identifiers,ans)
    if(flag == -1):
        ans = joinvalues
    flag1 = groupbyclause(identifiers,ans,selectcolumns,flag)
    flag2 = aggregate(identifiers,ans,selectcolumns,groupbydata,aggregatecolumn,aggregatefunction,columnsjoin,flag1) 
    if((flag2 == -1 and flag1 == 0)) :
        for keys in groupbydata:
                result.append(keys)
    elif(flag2 == -1 and flag1 == -1 and flag == 0):
        result = ans
    elif(flag2 == -1 and flag1 == -1 and flag == -1):
        result = joinvalues  
    flag3 = orderby(identifiers,result,selectcolumns,columnsjoin)     
    outputcolumns = []
    outputcolumns1 = []
    tablename = ""
    if(flag1 == 0):
        outoutcolumns1 = []
        for values in selectcolumns:
          if(len(values.split('(')) == 2):
            col = (values.split('(')[1].split(')')[0])
            for tot in tables:
                if col in tables[tot]:
                   outputcolumns1.append(values)
                   break
                
          else:
            for tot in tables:
               if values in tables[tot]:
                   sot = tot+"."+values.lower()
                   outputcolumns1.append(sot)
                   break
        for values in range(0,len(outputcolumns1)):
            if(values == len(outputcolumns1)-1):
                print(outputcolumns1[values],end='\n')
            else:
                print(outputcolumns1[values],end=',')
        if(isinstance(result[0],list) == True):            
          for values in range(0,len(result)):
            for j in range(0,len(result[values])):
                if(j == len(result[values])-1):
                    print(result[values][j],end='\n')
                else:
                    print(result[values][j],end=',') 
        else:
          for i in range (0,len(result)):
            if(i == len(result) -1):
                print(result[i],end = '\n')
            else:
                print(result[i],end = ',')  

        sys.exit(0)
    if(stat != 1):
     for values in selectcolumns:
       if(len(values.split('(')) == 2):
           col = (values.split('(')[1].split(')')[0])
           if (col == '*'):
               outputcolumns.append(values)
           else:    
             for tot in tables:
               if col in tables[tot]:
                   outputcolumns.append(values)
                   break        
       else:
            for tot in tables:
               if values in tables[tot]:
                   sot = values
                   outputcolumns.append(sot)
                   break                  
    midlist = [] 
    if (stat == 1 and flag1 == 0):
        print("Wrong Query")
        sys.exit(0)
    if(stat == 1):
        for values in columnsjoin:
            for tot in tables:
               if values in tables[tot]:
                   sot = values
                   outputcolumns.append(sot)
        for tot in outputcolumns:
            for values in tables:
                if(tot in tables[values]):
                    sot = values+"."+tot.lower()
                    outputcolumns1.append(sot)
                    break
        for values in range(0,len(outputcolumns1)):
            if(values == len(outputcolumns1) -1):
                print(outputcolumns1[values],end = '\n')
            else:
                print(outputcolumns1[values],end = ',')    
        for values in range (0,len(result)):
            for j in range(0,len(result[values])):
                if(j == len(result[values])-1):
                    print(result[values][j],end = '\n')
                else:
                    print(result[values][j],end = ',')

        sys.exit(0)                          
    if((isinstance(result[0],list) == True) and stat != 1):
        for values in result:
            midlist1 = []
            for tot in outputcolumns:
              indexes = columnsjoin.index(tot.upper())
              midlist1.append(values[indexes])             
            midlist.append(midlist1)
        result = midlist  
        for tot in outputcolumns:
            for values in tables:
                if(len(tot.split('('))==2):
                    col = tot.split('(')[1].split(')')[0]
                    if(col == '*'):
                        outputcolumns1.append(tot)
                        break
                    else:    
                     if(col in tables[values]):
                        outputcolumns1.append(tot)
                        break    
                else:
                    if(tot in tables[values]):
                        col = values+"."+tot.lower()
                        outputcolumns1.append(col)
                        break
    else:
        for tot in outputcolumns:
            for values in tables:
                if(len(tot.split('('))==2):
                    col = tot.split('(')[1].split(')')[0]
                    if (col == "*"):
                        outputcolumns1.append(tot)
                        break
                    else:    
                     if(col in tables[values]):
                        outputcolumns1.append(tot)
                        break    
                else:
                    if(tot in tables[values]):
                        col = values+"."+tot.lower()
                        outputcolumns1.append(col)
                        break 
                             
    if(dist == True):
        map1 = []
        tit = []
        for val in result:
            if (val not in map1):
                tit.append(val)
                map1.append(val)    
        result = tit     
    for i in range (0,len(outputcolumns1)):
        if(i == len(outputcolumns1)-1):
            print(outputcolumns1[i],end = '\n')
        else:
            print(outputcolumns1[i],end = ',')    
    if(isinstance(result[0],list) == True):
     for v in range(0,len(result)):
        for j in range(0,len(result[v])):
            if(j == len(result[v])-1):
                print(result[v][j],end = '\n')
            else:
                print(result[v][j],end = ',')    
    else:
        for i in range (0,len(result)):
            if(i == len(result) -1):
                print(result[i],end = '\n')
            else:
                print(result[i],end = ',')        
def orderby(identifiers,result,selectcolumns,columnsjoin):
    orderbyval = ""
    groupclause = ""
    flag1 = 0
    if((isinstance(result[0],list) == False) ):
        return 0
    for values in identifiers:
        if (values == "ORDER" ):
            orderbyval = values
            flag1 = 1
        elif (values == "ORDER BY"):
            orderbyval = values
    if(orderbyval == ""):
        return -1
    i = 0
    while(i<len(identifiers)):
        if(identifiers[i] == orderbyval):
            break            
        i = i+1
    if(flag1 == 1):
        i = i+2
    else:
        i = i+1
    orderbyval = identifiers[i]
    order = ""
    orderbyval1 = ""
    if(len(orderbyval.split(" "))==2):
        orderbyval1 = orderbyval.split(" ")[0]
        order = orderbyval.split(" ")[1]
    else:
        orderbyval1 = orderbyval    
    i = 0
    flag2 = 0
    for values in identifiers:
        if (values == "GROUP" ):
            groupclause = values
            flag2 = 1
        elif (values == "GROUP BY"):
            groupclause = values
    if(groupclause != ""):
      while(i<len(identifiers)):
        if(identifiers[i] == groupclause):
            break            
        i = i+1
      if(flag2 == 1):
        i = i+2
      else:
        i = i+1 
      groupclause = identifiers[i]
    if(groupclause != "" and groupclause != orderbyval1):
        print("Wrong value in orderby")
        sys.exit(0)
    elif(groupclause != "" and groupclause == orderbyval1):    
       indexes = selectcolumns.index(orderbyval1)
    elif(groupclause == ""):
        indexes = columnsjoin.index(orderbyval1)  
         
    if(order == "DESC"):
        result.sort(key=lambda x: x[indexes],reverse = True)
    else:    
        result.sort(key=lambda x: x[indexes])
    return 0
def aggregate(identifiers,ans,selectcolumns,groupbydata,aggregatecolumn,aggregatefunction,columnsjoin,flag1):
    flag = 0
    count = 0
    for values in selectcolumns:
          if(len(values.split('(')) == 2):
            aggregatefunction.append(values.split('(')[0])
            aggregatecolumn.append(values.split('(')[1].split(')')[0])
          else:
              aggregatefunction.append("Cheems")
              aggregatecolumn.append(values)
              count = count+1
    if(len(aggregatecolumn) == count):
        return -1    
    if(len(groupbydata) == 0):
        if(len(ans)!=0):
            print("Invalid Query")
            sys.exit(0)
        else:
            return -1 
    if(flag == 1):
        cq = []
        cq = transpose(ans,cq)
        for i in range(0,len(aggregatecolumn)):
            c = aggregatecolumn[i]
            indexes = 0
            if(c != "*"):
              indexes = columnsjoin.index(c)
            func = aggregatefunction[i]
            if(func == "MAX" or func == "max"):
                result.append(max(cq[indexes]))
            elif (func == "SUM" or func == "sum"):
                result.append(sum(cq[indexes]))    
            elif(func == "MIN" or func == "min"):
                result.append(min(cq[indexes]))
            elif((func == "COUNT" or func == "count") and c == "*"):
                result.append(len(ans))
            elif((func == "COUNT" or func == "count") and c != "*"):
                result.append(len(cq[indexes]))
            elif(func == "AVG" or func == "avg" or func == "AVERAGE" or func == "average"):
                result.append(sum(cq[indexes])/len(cq[indexes]))
    else:
        for i in groupbydata:
            cq = [] 
            tq = []
            cq = groupbydata[i]
            if len(cq) == 1:
                cq = list(cq)
            tq = transpose(cq,tq)
            tt = []
            for j in range(0,len(aggregatecolumn)):
                c = aggregatecolumn[j]
                indexes = 0
                if(c != "*" and c!= "Cheems"):
                  c = c.replace(" ","")
                  indexes = columnsjoin.index(c)
                func = aggregatefunction[j]
                if(func == "max" or func == "MAX"):
                    tt.append(max(tq[indexes]))
                elif(func == "min" or func == "MIN"):
                    tt.append(min(tq[indexes]))
                elif (func == "SUM" or func == "sum"):
                    tt.append(sum(tq[indexes]))     
                elif((func == "count" or func == "COUNT") and c == "*"):
                    tt.append(len(cq))
                elif((func == "COUNT" or func == "count") and c != "*"):
                    tt.append(len(tq[indexes]))

                elif(func == "avg" or func == "AVG"):
                    tt.append(sum(tq[indexes])/len(tq[indexes]))
                elif(func == "Cheems"):
                    tt.append(i)
            result.append(tt)            
    return 0         
def groupbyclause(identifiers,ans,selectcolumns,flag):
    groupclause = ""
    flag1 = 0
    for values in identifiers:
        if (values == "GROUP" ):
            groupclause = values
            flag1 = 1
        elif (values == "GROUP BY"):
            groupclause = values
    if(groupclause == ""):
        return -1      
    if(len(ans) == 0 and flag == -1):
        for i in joinvalues:
            ans.append(i)
    i = 0
    while(i<len(identifiers)):
        if(identifiers[i] == groupclause):
            break
        i = i+1
    if(flag1 == 1):
        i = i+2
    else:
        i = i+1   
    groupclause = identifiers[i]   
    for values in selectcolumns:
            indexes = -1
            values = values.replace(" ","")
            if(len(values.split('(')) != 2):         
              if(values != groupclause):
                print("Wrong column in query1")
                sys.exit(0)
    indexes =  columnsjoin.index(groupclause)
    for values in ans:
        groupbydata[values[indexes]] = []
    for values in ans:
        midvalues = []
        for i in range(0,len(values)):
                midvalues.append(values[i])    
        groupbydata[values[indexes]].append(midvalues)    
    return 0                
def crossjoin(identifiers,joinvalues):
       i = 0
       while (i<len(identifiers)):
           if(identifiers[i] == 'from' or identifiers[i] == 'FROM'):
               break
           i = i+1
       tablesname = identifiers[i+1]     
       tablesjoin = tablesname.split(',')
       tq = [] 
       tablesjoinval = []
       for values in tablesjoin:
           pq = []
           for keyvalues in csvdata[values]:
               columnsjoin.append(keyvalues)
               pq.append(csvdata[values][keyvalues])
           cq = []
           cq = transpose(pq,cq)
           tq.append(cq) 
       import itertools        
       for elements in itertools.product(*tq):
               joinvalues.append(sum(list((elements)),[]))                               
def transpose(pq,cq):
    cq = list(map(list, zip(*pq)))
    return cq
def whereclause(identifiers,ans):
    i = 0
    while(i<len(identifiers)):
        if(identifiers[i]== "FROM"):
            break
        i = i+1
    if(i+2<len(identifiers)):
        i = i+2
    wherequery = str(identifiers[i])
    if(wherequery[0] != 'W'):
        return -1
    andno = 0 
    orno = 0
    wherequery = wherequery.replace(" ","")
    query = wherequery.split("WHERE")[1]
    condition1 = ""
    condition2 = ""
    if(len(query.split("AND")) == 2):
        andno  = 1
        condition1 = query.split("AND")[0]
        condition2 = query.split("AND")[1]
    elif(len(query.split("OR")) == 2):
        orno = 1
        condition1 = query.split("OR")[0]
        condition2 = query.split("OR")[1]
    else:
        condition1 = query     
    condition1 = condition1.replace(" ","")
    condition2 = condition2.replace(" ","")     
    table1 = ""
    table2 = ""
    table3 = ""
    table4 = ""
    operator1 = ""
    operator2 = ""
    for values in operators:
        if(len(condition1.split(values)) == 2):
            operator1 = values
            table1 = condition1.split(values)[0]
            table2 = condition1.split(values)[1]
            break
    if(condition2 != ""):
        for values in operators:
            if(len(condition2.split(values)) == 2):
                operator2 = values
                table3 = condition2.split(values)[0]
                table4 = condition2.split(values)[1] 
                break                     
    flag1 = 0
    flag2 = 0
    flag3 = 0
    flag4 = 0  
    length = len(columnsjoin) 
    for values in columnsjoin:
            if(table1 != "" and values == table1):
                flag1 = 1
            elif(table2 != "" and values == table2):
                flag2 = 1
            elif(table3 != "" and values == table3):
                flag3 = 1
            elif(table4 != "" and values == table4):
                flag4 = 1
    if(table1 != "" and flag1 == 0):
        print("column named" + table1 + "Not present")
        sys.exit(0)
    if(table2 != "" and flag2 == 0 and table2[0] != "-" and table2[0] != "0" and table2[0] != "1" and table2[0] != "2" and table2[0] != "3"and table2[0] != "4"and table2[0] != "5"and table2[0] != "6"and table2[0] != "7"and table2[0] != "8"and table2[0] != "9"):
        print("column named" + table2 + "Not present")
        sys.exit(0)
    if(table3 != "" and flag3 == 0):
        print("column named" + table3 + "Not present")
        sys.exit(0)
    if(table4 != "" and flag4 == 0 and table4[0] != "-" and table4[0] != "0"and table4[0] != "1"and table4[0] != "2"and table4[0] != "3"and table4[0] != "4"and table4[0] != "5"and table4[0] != "6"and table4[0] != "7"and table4[0] != "8"and table4[0] != "9"):
        print("column named" + table4 + "Not present")
        sys.ext(0) 
    index1 = ""
    index2 = ""
    index3 = ""
    index4 = ""
    for i in range(0,len(columnsjoin)):
        if(columnsjoin[i] == table1):
            index1 = i
        elif(columnsjoin[i] == table2):
            index2 = i
        elif(columnsjoin[i] == table3):
            index3 = i
        elif(columnsjoin[i] == table4):
            index4 = i
    indflag1 = 0
    indflag2 = 0
    if(index2 == ""):
        indflag1 = 1
    if(index4 == ""):
        indflag2 = 1               
    ops = { "<": operator.lt, ">": operator.gt,"<=": operator.le, ">=": operator.ge,"=": operator.eq }       
    for i in joinvalues:
        if(andno == 1):
            if(indflag1 == 0 and indflag2 == 0 and    (ops[operator1](int(i[index1]),int(i[index2])))   and    (ops[operator2](int(i[index3]),int(i[index4])))):
                ans.append(i) 
            elif(indflag1 == 1 and indflag2 == 0 and (ops[operator1](int(i[index1]),int(table2)))   and    (ops[operator2](int(i[index3]),int(i[index4])))):
                ans.append(i)
            elif(indflag1 == 0 and indflag2 == 1 and (ops[operator1](int(i[index1]),int(i[index2])))   and    (ops[operator2](int(i[index3]),int(table4)))):
                ans.append(i)
            elif(indflag1 == 1 and indflag2 == 1 and (ops[operator1](int(i[index1]),int(table2)))   and    (ops[operator2](int(i[index3]),int(table4)))):
                ans.append(i)            
        elif(orno == 1):
            if( indflag1 == 0 and indflag2 == 0 and    ((ops[operator1](int(i[index1]),int(i[index2])))   or    (ops[operator2](int(i[index3]),int(i[index4])))) ):
                ans.append(i)
            elif(indflag1 == 1 and indflag2 == 0 and ((ops[operator1](int(i[index1]),int(table2)))   or    (ops[operator2](int(i[index3]),int(i[index4]))))):
                ans.append(i)
            elif(indflag1 == 0 and indflag2 == 1 and ((ops[operator1](int(i[index1]),int(i[index2])))   or    (ops[operator2](int(i[index3]),int(table4))))):
                ans.append(i)
            elif(indflag1 == 1 and indflag2 == 1 and ((ops[operator1](int(i[index1]),int(table2)))   or    (ops[operator2](int(i[index3]),int(table4))))):
                ans.append(i)    
        elif(andno == 0 and orno == 0 and indflag1 == 0):
            if  (ops[operator1](int(i[index1]),int(i[index2]))):
                ans.append(i) 
        elif(andno == 0 and orno == 0 and indflag1 == 1):
            if(ops[operator1](int(i[index1]),int(table2))):
                ans.append(i)
    return 0                                                                     
if __name__ =="__main__":
    inputsql = sys.argv[1]
    readtxt()
    csvparse()
    parsequery(inputsql,identifiers, result,ans,tables,joinvalues)
