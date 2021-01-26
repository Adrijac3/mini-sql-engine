import sys
import sqlparse
import itertools
import re

'''Global variables'''
product=[]   #to store the cartesian product of tables passed in query
productHeaders=[]   #to store the schema of joined tables
schema={}   #to store schema of all the tables (key as tableName, value as list of columnNames)
database={}   #to store row-wise data of all tables (key as tableName, value as list of list of rows)
functions=[]  #to store pair of aggregate function and its column, if present in query
validColumnsInDatabase=[]    #to store a list of all unique columns present in the database
validColumnsInDatabase.append("*")
'''Reference list'''
AGGREGATE_FUNCTIONS=['AVG', 'MAX', 'MIN', 'SUM', 'COUNT']
OPERATORS=['<=','>=', '!=', '==','=', '>', '<']

#####Function to create table schemas from metadata.txt file#####
def getTableSchema():
    try:
        f= open("metadata.txt","r")
    except IOError:
        sys.stderr.write("Error: No metadata file found.\n")
        sys.exit()
    tableNameAhead=0            #flag to denote we have a tableName coming in next line
    while True:
        l= f.readline()
        if not l:
            break
        l= l.strip("\n")        #remove "\n" from the line as each line is delimited with it when read from file
        if l=="<begin_table>":
            tableNameAhead=1    #begin_table will be followed by a tableName, so set flag to 1
            tableName=""
        elif tableNameAhead==1:
            tableName=l
            tableNameAhead=0    #reset flag to 0 since after this we will be getting attributes
            colList=[]          #create a list to store incoming attributes next
        elif l=="<end_table>":  
            schema[tableName]=colList   #store all atributes collected till now in colList to its tableName (key)
        else:
            colList.append(l)   #keep appending attributes into the colList
            validColumnsInDatabase.append(l)
    f.close()
    return

#####Function to fill the database with values from csv file#####
def makeDatabase(schema):
    for tableName in schema.keys():
        try:
            f=open(tableName+".csv","r")
        except IOError:
            sys.stderr.write("Error: "+tableName+".csv file not found.\n")
            sys.exit()
        rows=[]
        while True:
            l=f.readline()
            if not l:
                break
            l=l.strip("\n")
            row=l.split(',')
            int_row=[]
            for val in row:
                val=val.strip("\'\"")
                int_row.append(int(val))
            rows.append(int_row)
        f.close()
        database[tableName]=rows
    return

#####Extract identifiers from given query####
def TokenizeQuery(query):
    input_query=sys.argv[1]
    if not input_query[-1]==";":
        sys.stderr.write("SQL query should end with ;\n")
        sys.exit()
    input_query=input_query[:-1]
    query=sqlparse.format(input_query,keyword_case='upper')
    parsed = sqlparse.parse(query)[0].tokens
    statement=sqlparse.sql.IdentifierList(parsed).get_identifiers()
    queryTokens=[]
    for s in statement:
            queryTokens.append(str(s))
    return queryTokens

# {'table1':[a,b,c],'table2':[d,e]}
####Function to error check and handle column_names and make list of aggregate function if any####
def handleCols(col_list):
    #print("cols passed with select",col_list)
    if "*" in col_list:
        return col_list
    loneColumns=[]
    for c in col_list:
        c=c.strip()
        if '(' in c:
            c= c.split('(')
            if c[1][:-1] not in validColumnsInDatabase:
                sys.stderr.write("Error: invalid column name "+c[1][:-1]+"\n")
                sys.exit()
            functions.append([c[0],c[1][:-1]])
        else:
            if c not in validColumnsInDatabase:
                sys.stderr.write("Error: invalid column name "+str(c)+"\n")
                sys.exit()
            loneColumns.append(c)
    #print("agg function list=",functions)
    return loneColumns

####Function to check if passed tables and columns exist in database####
def checkTablesandCols(tables, cols):
    #print("tables passed in query=",tables)
    #print("columns passed in query=",cols)
    for table in tables:
        if table not in schema:
            sys.stderr.write("Error: table doesn't exist "+table+"\n")
            sys.exit()
    return handleCols(cols)

####Error handling and processing the query supplied#####
def processQuery(queryTokens):
    tables=[]       #to store all tables passed in query
    cols=[]          #to store all columns passed in query
    distinct_flag=0  #to denote is dinstinct keyword is present
    clauses=[]       #to store clauses from where and after, if any
    if len(queryTokens)<4:
        sys.stderr.write("Error: too small query\n")
        sys.exit()
    if queryTokens[0]!="SELECT":
        sys.stderr.write("Error: First clause of query should be SELECT\n")
        sys.exit()
    if (queryTokens[2]!="FROM" and queryTokens[1]!="DISTINCT") or (queryTokens[1]=="DISTINCT" and queryTokens[3]!="FROM"):
        sys.stderr.write("Error: query should contain FROM or correct spelling of distinct (if provided)\n")
        sys.exit()
    if queryTokens[1]=="DISTINCT":
        distinct_flag=1
        tables_list=queryTokens[4]
        col_list=queryTokens[2]
    else:
        tables_list=queryTokens[3]
        col_list=queryTokens[1].replace(" ","")
    if col_list==[] or tables_list==[]:
        sys.stderr.write("Error: either column name(s) or table name(s) is missing\n")
        sys.exit()
    tables=tables_list.replace(" ","").split(",")
    cols=col_list.split(",")
    selectList=[]
    for c in cols:
        selectList.append(c.strip())
    cols=checkTablesandCols(tables, cols)   #to see if tables and columns are valid
    if distinct_flag==1:
        if len(queryTokens)<=5:
            pass
        else:
            for i in range(5, len(queryTokens)):
                clauses.append(queryTokens[i])
    else:
        if len(queryTokens)<=4:
            pass
        else:
            for i in range(4, len(queryTokens)):
                clauses.append(queryTokens[i])       
    executeQuery(tables,cols,distinct_flag,clauses,selectList)

####Function to display table####
def display(table):
    for i in range(0,len(table)):
        for j in range(len(table[i])):
            if j==len(table[i])-1:
                print(table[i][j],end='\n')
            else:
                print(table[i][j],end=',')
        #print(table[i])

####Function to join tables####
def join(tables):
    db=[]
    productSchema=[]
    #print("tables passed in join function:",tables)
    for tablename in tables:
        productSchema.append(schema[tablename])
        db.append(database[tablename])
    for elements in itertools.product(*db):
        product.append(sum(list((elements)),[]))
    #display(product)
    #print(productSchema)
    for i in range(len(productSchema)):
        for col in productSchema[i]:
            productHeaders.append(col)
    #print("schema of joined table=",productHeaders)

####Simple Linear search to get index of a string from a list of strings####
def getIndex(str):
    for i in range(len(productHeaders)):
        if productHeaders[i]==str:
            return i

def fetchWhereConditions(conditionStr,opList):
    conditionL=[]
    opFound=False
    for operator in OPERATORS:
            if operator in conditionStr:
                if operator=="=":
                    opList.append("==")
                else:
                    opList.append(operator)
                conditionL=conditionStr.split(operator)
                #print("conditionL",conditionL)
                if len(conditionL)>2:
                    sys.stderr.write("Error. Too many operators\n")
                    sys,exit()
                for i in range(len(conditionL)):
                    conditionL[i]=conditionL[i].strip()
                opFound=True
                #print(opFound)
                break
    #print("conditionL",conditionL)
    if opFound==False:
        sys.stderr.write("Error:Operator not supported or invalid\n")
        sys.exit()
    return conditionL

####Function to process where and its conditions####
def processWhere(whereClause):
    #print(whereClause)
    whereClause=whereClause[6:]
    conditionList=[]
    opList=[]
    relop=""
    singleCond=0
    if whereClause.find("AND")==-1 and whereClause.find("OR")==-1:          #single condition
        conditionList.append(fetchWhereConditions(whereClause,opList))
        singleCond=1
    elif whereClause.find("AND")!=-1:
        relop="AND"
    elif whereClause.find("OR")!=-1:
        relop="OR"
    else:
        sys.stderr.write("Error: Only AND and OR relational operator supported\n")
        sys.exit()
    if relop!="":
        whereClause=whereClause.split(relop)
    #print(whereClause)
    if singleCond==0:
        if len(whereClause)>3 and singleCond==0:
            sys.stderr.write("Error: Operator not supported or invalid\n")
            sys.exit()
        else:
            conditionList.append(fetchWhereConditions(whereClause[0],opList))
            conditionList.append(fetchWhereConditions(whereClause[1],opList))
    #print("conditionlist=",conditionList)
    #print(opList)
    finalAns=[]
    if conditionList[0][0] not in productHeaders:
        sys.stderr.write("Error: wrong col names")
        sys.exit()
    for rows in product:
        if re.match('^[0-9]*$', conditionList[0][1]) or re.match('^-[0-9]*$', conditionList[0][1]):
            exp=str(rows[getIndex(conditionList[0][0])])+str(opList[0])+conditionList[0][1]
        else:
            if conditionList[0][1] not in productHeaders:
                sys.stderr.write("Error: wrong col names\n")
                sys.exit()
            else:
                exp=str(rows[getIndex(conditionList[0][0])])+str(opList[0])+str(rows[getIndex(conditionList[0][1])])
        if len(opList)==2:
            if conditionList[1][0] not in productHeaders:
                sys.stderr.write("Error: wrong col names\n")
                sys.exit()
            if re.match('^[0-9]*$', conditionList[1][1]):
                exp+=" "+relop.lower()+" "+str(rows[getIndex(conditionList[1][0])])+str(opList[1])+conditionList[1][1]
            else:
                if conditionList[1][1] not in productHeaders:
                    sys.stderr.write("Error: wrong col names")
                    sys.exit()
                else:
                    exp+=" "+relop.lower()+" "+str(rows[getIndex(conditionList[1][0])])+str(opList[1])+str(rows[getIndex(conditionList[1][1])])
        if eval(exp)==True:
            finalAns.append(rows)
    return finalAns

####Function to execute Group By####
def processGroupBy(clauses, cols, table):           #clauses=['GROUP BY', 'colname'] cols=list of columns in select(lone cols)
    # print("clauses passed in group by=",clauses)
    # print("columns present in query are=",cols)
    if cols==[]:
        sys.stderr.write("Error: Query doesn't make sense. So not handled!!!\n")
        sys.exit()
    groupCol=clauses[1].split(',')
    #print("column present in group by are=",groupCol)
    if len(groupCol)!=len(cols):
        sys.stderr.write("Error: mismatch of columns in groupby and select\n")
        sys.exit()
    for c in cols:
        if c not in groupCol:
            sys.stderr.write("Query doesn't make sense. Aborting!!!\n")
            sys.exit()
    groupedAns={}
    colIndex=getIndex(groupCol[0])
    for row in table:
        if row[colIndex] not in groupedAns:
            groupedAns[row[colIndex]]=[]
        groupedAns[row[colIndex]].append(row)
    #print(groupedAns)
    return groupedAns
    
####Function to handle aggregate functions####
def aggregate(input):
    #print("input in agg func is=",input)
    result=[]
    if type(input) is dict:
        for groupByCol in input:
            transposeList=[]
            transposeList=list(map(list,zip(*input[groupByCol])))
            #print("transpose list of group by",groupByCol,"is= ",transposeList)
            if functions==[]:
                result.append([groupByCol])
                #print("Reuturn extracted list")
            else:
                temp=[]
                temp.append(groupByCol)
                for func in functions:
                    action=func[0].upper()
                    if func[1].find("*")==-1:
                       colPos=getIndex(func[1])
                    else:
                        colPos=0
                    #print(action,colPos)
                    if action=="MAX":
                        temp.append(max(transposeList[colPos]))
                    elif action=="MIN":
                        temp.append(min(transposeList[colPos]))
                    elif action=="SUM":
                        temp.append(sum(transposeList[colPos]))
                    elif action=="AVG":
                        temp.append(sum(transposeList[colPos])/len(transposeList[colPos]))
                    elif action=="COUNT":
                        temp.append(len(transposeList[colPos]))
                result.append(temp)
        # print("Returing list after applying agg funcs")
        # print("result=",result)
        return result

    if type(input) is list:
        if input==[]:
            return input
        transposeList=[]
        transposeList=list(map(list,zip(*input)))
        #print(transposeList)
        if functions==[]:
            #print("returning list as it is")
            return input
        else:
            for func in functions:
                action=func[0].upper()
                colPos=getIndex(func[1])
                #print(action,colPos)
                if action=="MAX":
                    result.append(max(transposeList[colPos]))
                elif action=="MIN":
                    result.append(min(transposeList[colPos]))
                elif action=="SUM":
                    result.append(sum(transposeList[colPos]))
                elif action=="AVG":
                    result.append(sum(transposeList[colPos])/len(transposeList[colPos]))
                elif action=="COUNT":
                    result.append(len(input))
            # print("Returing list after applying agg funcs")
            # print("result=",result)
            return [result]

####Function to execute Order By####
def processOrderBy(clauses,table,selectList,grouped):
    # print("table received by order by is:", table)
    # print("clause received in order by ", clauses)
    # print("select list received in order by ", selectList)
    if functions!=[]:
        count=0
        for s in selectList:
            if s.find("(")!=-1:
                count+=1
        if len(selectList)==count:
            return table
    Asc=1
    orderCols=""
    if clauses[1].find("ASC")!=-1:
        orderCols=clauses[1].replace("ASC","").strip()
        # print(orderCols)
    elif clauses[1].find("DESC")!=-1:
        orderCols=clauses[1].replace("DESC","").strip()
        Asc=0
    else: orderCols=clauses[1]
    #print(orderCols)
    if orderCols not in productHeaders:
        sys.stderr.write("Column not in this table\n")
        sys.exit()
    # print("oooooo")
    # print(orderCols)
    #orderCols=orderCols.split(",")
    if grouped==1:
        orderByCol=0
    else:
        orderByCol=getIndex(orderCols)
    # print("Ordering index on table is=",orderByCol)
    if Asc==0:
        table = sorted(table, key=lambda col: col[orderByCol],reverse = True)
    else:
        table = sorted(table, key=lambda col: col[orderByCol])
    # print("table after ordering=",table)
    return table

####Function to apply distinct on table####
def distinct(table):
    list2=[]
    for l in table:
        if l not in list2:
            list2.append(l)
        else:
            continue
    return list2

####Checks whether both groupby and orderby have same columns or not####
def checkOrderAndGroup(groupCols,orderCols):
    if len(groupCols.split(","))>1:
        sys.stderr.write("Sorry: Only 1 column can be grouped in limited sql engine. Aborting!!!\n")
        sys.exit()
    if orderCols.find("ASC")!=-1:
        orderCols=orderCols.replace("ASC","").strip()
    elif orderCols.find("DESC")!=-1:
        orderCols=orderCols.replace("DESC","").strip()
    # print("groupCols=",groupCols,"OrderCols=",orderCols)
    if groupCols!=orderCols:
        sys.stderr.write("Error: group by and order by should have same columns\n")
        sys.exit()

def projectHelper(table, cols, selectList,grouped):
    ultimate=[]
    # print(cols)
    # print(selectList)
    if functions==[]:         #no aggregate function
        if grouped==1:  #group by present. So query has single lone column. select X from t group by X
            return table
        else:           #group by absent.
            for row in table:
                temp=[]
                for c in cols:
                    temp.append(row[getIndex(c)])
                ultimate.append(temp)
            return ultimate
    else:                    # aggregate function present
        if cols==[]:         # no lone column present
            return table
        else:                #lone col present
            if selectList.index(cols[0])==0:     #Same order of queries as stored in table
                return table
            else:                       #lone column not in first position
                for row in table:
                    temp=[]
                    flag=0
                    i=1
                    for c in selectList:
                        if c!=cols[0] and flag==0:
                            temp.append(row[i])
                            i+=1
                        elif c==cols[0]:
                            flag=1
                            temp.append(row[0])
                        else:
                            temp.append(row[i])
                            i+=1
                    ultimate.append(temp)
                return ultimate

def beautifyTable(table, selectList,cols,grouped):     #grouped->remove
    displayHeader=[]
    if selectList[0]=="*":
        displayHeader=cols
    else:
        displayHeader=selectList
    # print("displayHeader=",displayHeader)
    # print("lone cols=",cols)
    for i in range(len(displayHeader)):
        if displayHeader[i] in cols:
            #print("displayHeader[i]=",displayHeader[i])
            for t in schema.keys():
                #print("inside schema loop")
                if displayHeader[i] in schema[t]:
                    #print("tablename=",t)
                    displayHeader[i]=t+"."+displayHeader[i]
                    #print("displayHeader[i]=",displayHeader[i])
    for i in range(len(displayHeader)):
        displayHeader[i]=displayHeader[i]
    for i in range(len(displayHeader)):
        if i==len(displayHeader)-1:
            print(displayHeader[i],end='\n')
        else:
            print(displayHeader[i],end=',')
    #print(displayHeader)
    display(table)
    
    exit
####Function to execute query####  
def executeQuery(tables, cols, distinct_flag, clauses, selectList):
    join(tables)
    #display(product)
    grouped=0
    if '*' in cols:
        cols=[]
        for col in productHeaders:
            cols.append(col)
    for c in cols:
        if c not in productHeaders:
            sys.stderr.write("Column not in this table\n")
            sys.exit()
    newTable=[]
    if clauses==[]:                   #select MAX(A) from t1,t2 or select a,b from t1,t2
        if  functions!=[] and cols!=[]:
            sys.stderr.write("Error:Lone columns cannot exist in select without group by, when agregate functions are present\n")
            sys.exit()
        newTable=aggregate(product)
    #print("clauses after joining tables are=",clauses)
    if len(clauses)==1:         #it should only have ['WHERE COND1 AND/OR COND2']
        if functions!=[] and cols!=[]:
            sys.stderr.write("Error:Lone columns cannot exist in select without group by, when agregate functions are present\n")
            sys.exit()
        finalAns=processWhere(clauses[0])
        #print("table after applying where=")
        #display(finalAns)
        newTable=aggregate(finalAns)
    if len(clauses)==2:         # ['ORDER BY','Cols'] or ['GROUP BY','Cols']
        if clauses[0]=='GROUP BY':
            grouped=1
            groupedDict=processGroupBy(clauses,cols,product)
            newTable=aggregate(groupedDict)
        elif clauses[0]=='ORDER BY':
            if functions!=[] and cols!=[]:
                sys.stderr.write("Error:Lone columns cannot exist in select without group by, when agregate functions are present")
                sys.exit()
            newTable=aggregate(product)
            newTable=processOrderBy(clauses,newTable,selectList,0)
        else:
            sys.stderr.write("Error: where should follow group by and/or order by only\n")
            sys.exit()
    if len(clauses)==3:        # ['WHERE CONDS', 'GROUP BY', 'Cols'] or ['WHERE CONDS', 'ORDER BY', 'Cols']
        finalAns=processWhere(clauses[0])
        #print("table after applying where=")
        #display(finalAns)
        if clauses[1]=='GROUP BY':
            grouped=1
            groupedDict=processGroupBy(clauses[1:],cols,finalAns)
            newTable=aggregate(groupedDict)
        elif clauses[1]=='ORDER BY':
            newTable=aggregate(finalAns)
            newTable=processOrderBy(clauses[1:],newTable,selectList,0)
        else:
            sys.stderr.write("Error: where should follow group by and/or order by only\n")
            sys.exit()
    if len(clauses)==4:     #['GROUP BY', 'Cols', 'ORDER BY', 'Cols']
        if clauses[0]!="GROUP BY":
            sys.stderr.write("Error: Wrong sql query\n")
            sys.exit()
        if clauses[2]!="ORDER BY":
            sys.stderr.write("Error: Wrong sql query\n")
            sys.exit()
        grouped=1
        checkOrderAndGroup(clauses[1],clauses[3])
        groupedDict=processGroupBy(clauses[0:2],cols,product)
        newTable=aggregate(groupedDict)
        newTable=processOrderBy(clauses[2:],newTable,selectList,1)
    if len(clauses)==5:     # ['WHERE CONDS', 'GROUP BY', 'Cols', 'ORDER BY', 'Cols']
        finalAns=processWhere(clauses[0])
        #display(finalAns)
        if clauses[1]=='GROUP BY':
            grouped=1
            groupedDict=processGroupBy(clauses[1:3],cols,finalAns)
            newTable=aggregate(groupedDict)
        else:
            sys.stderr.write("Error: wrong sql query\n")
            sys.exit()
        if clauses[3]=='ORDER BY':
            checkOrderAndGroup(clauses[2],clauses[4])
            newTable=processOrderBy(clauses[3:5],newTable,selectList,1)
        else:
            sys.stderr.write("Error: wrong sql query\n")
            sys.exit()
    #print("!!!!!!!!!!!FINAL ANS!!!!!!!!!!")
    newTable=projectHelper(newTable,cols,selectList,grouped)
    if distinct_flag==1:
        newTable=distinct(newTable)
    beautifyTable(newTable,selectList,cols,grouped)
    #display(newTable)

def main():
    getTableSchema()
    #print(schema)
    makeDatabase(schema)
    #print(database)
    if not len(sys.argv)==2:
        sys.stderr.write("Error: Wrong input. Correct input syntax=> bash 2020201063.sh \"query\"\n")
        return
    queryTokens=TokenizeQuery(sys.argv[1])
    #print("query after tokenizing",queryTokens)
    processQuery(queryTokens)


if __name__ == "__main__":
    main()
