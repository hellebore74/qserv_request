"""
Some doc https://mysqlclient.readthedocs.io/index.html
"""

import sys
import subprocess
import re
import numpy as np
import collections

from astropy.table import Table, join

class QservQuery:
    """
    Simple class to connec to a DB and make queries. All returned queries are Astropy tables.
    """

    def __init__(self, **kwargs):

        # Connect to the data base
        self.user=kwargs["user"]
        self.host=kwargs["host"]
        self.port=kwargs["port"]
        self.dbName=kwargs["dbName"]

        self.mysqlCommand="mysql -P %d -h %s -u %s"%(self.port,self.host,self.user)

        self.paramNameConvDict=None
        self.tableNames=None

        if kwargs["convFileId"]:
            convModules=__import__("qservNameConversion_%s"%(kwargs["convFileId"]))
            self.paramNameConvDict=convModules.paramNameConvDict
            self.tableNames=list(convModules.paramNameConvDict.keys())

        # Create an empty dictionary where all restuls will be saved
        self.queries = {}
        self.tables = None

    def close(self):
        return

    def dbinfo(self):
        return


    def analyze_request(self,request):

        reqDict={}
        reqDict["request_init"]=request[:]
        reqDict["request"]=request[:]
        reqDict["paramList_init"]=None
        reqDict["paramList"]=None
        reqDict["paramList_comp"]=None
        reqDict["patternList_init"]=None
        reqDict["tables"]=None

        input_request="%s"%request
        
        # Try to remove unecessary whitespaces
        patternList=[(", ",","),("\n"," "),("\t"," "),("   "," "),("  "," ")]
        for p in patternList:
            old,new=p
            input_request=input_request.replace(old,new)

        #Add semicolon
        if not input_request.endswith(";"): input_request=input_request+";"
        print("input : ",input_request)
        
        request_low=input_request.lower()
        print(request_low)

        # Extract parameter list and db name
        iSelect=None
        try:
            iSelect=request_low.index("select")
        except:
            pass

        print("iSelect : ",iSelect)
        if iSelect==None:
            import pprint
            pprint.pprint(reqDict)
            return reqDict

        # Column list 
        iFrom=request_low.index("from")
        paramNames=(input_request[iSelect+len("select"):iFrom]).strip()
        if paramNames.lower().startswith("distinct"):
            paramNames=paramNames[len("distinct"):].strip()
            iSelect=iSelect+len("distinct")+1
        iSelect+=len("select ")

        print("param names : ",paramNames)
        paramList_init=None
        patternList_init=None
        if paramNames.startswith("["):
            patternList_init=tuple(x.strip() for x in paramNames[1:-1].split(",") if x!="")
        else:
            paramList_init=tuple(x.strip() for x in paramNames.split(",") if x!="")
        print("param list : ",paramList_init)
        print("pattern list : ",patternList_init)

        # Table names
        iWhere=None
        try:
            iWhere=request_low.index("where")        
        except:
            pass
        if iWhere==None: iWhere=len(input_request)
        tableInput=(input_request[iFrom+len("from"):iWhere]).strip()
        if tableInput.endswith(";"): tableInput=tableInput[:-1]
        print("table names : ",tableInput)
        tmp=[x.strip() for x in tableInput.split(",") if x!=""]
        tableNames={}
        for name in tmp:
            if not " as " in name.lower():
                tableNames[name]=name
            else:
                if " as " in name: name_db,alias_name=[x for x in name.split(" as ") if x!=""]
                else : name_db,alias_name=[x for x in name.split(" AS ") if x!=""]
                tableNames[name_db]=alias_name
        print("table names : ",tableNames)
        inv_tableNames = {v: k for k, v in tableNames.items()}

        # Where constraint
        whereRequest=None
        if iWhere<len(input_request):
            whereRequest=self.where_replace_columnnames_by_shortnames(input_request[iWhere:],tableNames)
            
        # Final param list 
        paramList=[]
        if paramList_init:
            for p in paramList_init:
                if p.lower() in ["*","count(*)"]:
                    paramList.append((p,list(tableNames.keys())[0],False,p))                
                elif not "." in p :
                    paramList.append((p,list(tableNames.keys())[0],False,p))
                else:
                    table,name=[x for x in p.split(".") if x!=""]
                    paramList.append((name,inv_tableNames[table],True,name))
        print("param list - normal : ",paramList)
        
        # Pattern list 
        if patternList_init:
            tableName=list(tableNames.keys())[0]
            pList=self.replace_pattern_wildcards(patternList_init,tableName)
            paramList=[(p,tableName,False,p) for p in pList]
            
        print("param list - pattern : ",paramList)

        # Replace column names by shorten column names
        paramList_short=self.replace_columnnames_by_shortnames(paramList,tableNames)
        print("param list - short : ",paramList_short)        
        
        # Check for dupplicate column names
        paramList_new,paramList_comp=self.check_for_column_duplicates(paramList_short,tableNames)
        print("param list - dupplicates : ",paramList_new)

        # Constraints
        reqDict["paramList_init"]=paramList_init
        reqDict["paramList_comp"]=paramList_comp
        reqDict["paramList"]=paramList_new
        reqDict["patternList_init"]=patternList_init
        reqDict["tables"]=tableNames

        new_request=reqDict["request_init"][:iSelect]+",".join(paramList_new)+" "+reqDict["request_init"][iFrom:]

        if whereRequest:
            iWhere=new_request.lower().find("where")
            new_request=new_request[:iWhere]+whereRequest.strip()

        if "where and" in new_request.lower():
            ipos=new_request.lower().find("where and")
            new_request=new_request[:ipos]+" WHERE "+new_request[ipos+len("where and"):]

        reqDict["request"]=new_request
        if not reqDict["request"].endswith(";"):reqDict["request"]=reqDict["request"]+";"
        import pprint
        pprint.pprint(reqDict)
        print("FINAL request : ",reqDict["request"])

        
        return reqDict
        

    def replace_pattern_wildcards(self,patternList,tableName):

        # Get parameter names corresponding to the patterns defined above
        paramList=[]
        for p in patternList:
            p=p.replace("*","\w+")
            if not p[-1]=="*": p=p+"$"
            tmp=[x for x in self.paramNameConvDict[tableName] if re.match(p,x)]
            paramList.extend(tmp)
        print("param list : ",paramList)
        
        # Update db request
        from collections import OrderedDict
        paramList=list(OrderedDict.fromkeys(paramList))

        return paramList


    def check_for_column_duplicates(self,paramList_input,tableNames):

        columnNames=[x for x,v,b,a in paramList_input]
        print("Columns : ",columnNames)
        print("Table names : ",tableNames)
        counter=collections.Counter(columnNames)
        
        paramList_new=paramList_input[:]
        paramList_comp=paramList_input[:]
        for i,p in enumerate(paramList_input):
            pName,tableName_real,bDot,pAlias=p
            tableName=tableNames[tableName_real]
            if not bDot:
                paramList_new[i]=pName
            else:
                if counter[pName]==1:
                    paramList_new[i]="%s.%s"%(tableName,pName)
                else:
                    paramList_new[i]="%s.%s as %s_%s"%(tableName,pName,tableName,pName)
                    paramList_comp[i]=(pName,tableName_real,bDot,"%s_%s"%(tableName,pName))

        return paramList_new,paramList_comp


    def replace_columnnames_by_shortnames(self,paramList_input,tableNames):

        if self.paramNameConvDict==None:
            return paramList_input
        
        # replaces parameter names by shorten parameter names starting
        #       with the longest parameter name
        paramList_new=[]
        for pName,tableName,bDot,pAlias in paramList_input:
            if tableName in self.paramNameConvDict:
                if pName in self.paramNameConvDict[tableName]:
                    pName=self.paramNameConvDict[tableName][pName]
            paramList_new.append((pName,tableName,bDot,pAlias))
                    
        return paramList_new


    def where_replace_columnnames_by_shortnames(self,whereReq,tableNames):

        inv_names = {v: k for k, v in tableNames.items()}                
        print(inv_names)
        reqTmp=re.split('(\W+)', whereReq)
        print("WHERE : ",reqTmp)
        reqTmp_new=reqTmp[:]
        for i,t in enumerate(reqTmp):
            if t!=".": continue
            tableName=reqTmp[i-1]
            if tableName in inv_names:
                tableName=inv_names[tableName]
                pName=reqTmp[i+1]
                if tableName in self.paramNameConvDict:
                    if pName in self.paramNameConvDict[tableName]:
                        pName=self.paramNameConvDict[tableName][pName]
                reqTmp_new[i+1]=pName

#        print("".join(reqTmp_new))
        return "".join(reqTmp_new)

    def execute_request(self,request):

        # Check if select --- from requets section contains wildcards
        request_init=request

        requestDict=self.analyze_request(request)
        request=requestDict["request"]
        tableNameDict=requestDict["tables"]
        paramComp=requestDict["paramList_comp"]
        
        # Final request
        if self.dbName: 
            cmd='%s %s -e "%s"'%(self.mysqlCommand,self.dbName,request)
        else:
            cmd='%s -e "%s"'%(self.mysqlCommand,request)
            
        cmd=cmd.replace("WHERE AND","WHERE")
        print(cmd)
        p=subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        res,err=p.communicate()
        print("res/err : ",err)
        if err: print("Error : ",err)

        exitcode=p.returncode
        if exitcode!=0: print(res,"\nExitCode : ",exitcode)
        if exitcode!=0: return [],[],[]

        # Get data types
        dataTypeConverter={}
        if tableNameDict:
            for tableName in tableNameDict:
                dataTypeConverter[tableName]={}
                request="show fields from %s"%tableName
                cmd='%s %s -e "%s"'%(self.mysqlCommand,self.dbName,request)
                print(cmd)
                p=subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                res_type,err_type=p.communicate()
                if err_type: print("Error : ",err_type)
                exitcode_type=p.returncode
                if exitcode_type!=0: print(res,"\nExitCode : ",exitcode_type)
                if exitcode_type!=0: return

                resTmp=res_type.decode("ascii")
                lines=[x for x in resTmp.split("\n") if x!=""]
                for l in lines[1:]:
                    tmp=[x for x in l.split("\t") if x!=""]
                    name,dType=tmp[0:2]
                    if dType=="tinyint(1)":
                        dataTypeConverter[tableName][tmp[0]]=bool
                    elif "float" in dType or "double" in dType:
                        dataTypeConverter[tableName][tmp[0]]=float
                    elif "int" in dType:
                        dataTypeConverter[tableName][tmp[0]]=int
                    elif "char" in dType:
                        dataTypeConverter[tableName][tmp[0]]=str
                    else:
                        print("UNDEFINED mysl data type : ",name," ",dType)
                        sys.exit()

#            print(dataTypeConverter)
                    

        # Decode request output
        resTmp=res.decode("ascii")
        if len(resTmp)==0:
            return [],[],[]

        # Extract parameter names and row values
        lines=[x for x in resTmp.split("\n") if x!=""]
        paramNames=[x for x in lines[0].split("\t") if x!=""]
        print(paramNames)
        paramTables=[]
        if paramComp:
            for p in paramNames:
                tmp=[(x[1],x[0]) for x in paramComp if x[-1]==p]
                if len(tmp)>0: paramTables.append(tmp[0])
                else: paramTables.append((list(tableNameDict.keys())[0],p))
        print(paramTables)

##        print("dataTypeConverter")
##        print(dataTypeConverter.keys())
##        for key in dataTypeConverter:
##            print(dataTypeConverter[key].keys())

        paramValueList=[]
        for i,l in enumerate(lines[1:]):
            if dataTypeConverter:
                vList=[]
                for j,x in enumerate(l.split("\t")):
                    pTable,pName=paramTables[j]
                    if pTable in dataTypeConverter and pName in dataTypeConverter[pTable]:
                        vList.append(dataTypeConverter[pTable][pName](x.strip()))
                    else:
                        vList.append(x.strip())

            else:
                vList=[x.strip() for x in l.split("\t")]
            paramValueList.append(tuple(vList))
#            if i==0:
#                for v in vList: print(v," ",type(v))

        # Replace shorten parameter names by real names
        paramNames_real=paramNames[:]
        if self.paramNameConvDict and tableNameDict:
            for tableName in tableNameDict:
                inv_names = {v: k for k, v in self.paramNameConvDict[tableName].items()}        
                for i,p in enumerate(paramNames):
                    if paramTables[i][0]==tableName:
                        if p in inv_names:
                            paramNames_real[i]=(inv_names[paramTables[i][1]])
        
        paramTypeList=[]
        if dataTypeConverter:
            for j,x in enumerate(paramNames):
                pTable,pName=paramTables[j]
                if pTable in dataTypeConverter and pName in dataTypeConverter[pTable]:
                    paramTypeList.append(dataTypeConverter[pTable][pName].__name__)
                else:
                    paramTypeList.append("str")
        
        print("Names : ",paramNames)
        print("Names - real : ",paramNames_real)
        print("Types : ",paramTypeList)

        return paramNames_real,paramValueList,paramTypeList


    def query(self, sqlquery, save=True, verbose=False):

        if verbose:
            print("Current query is")
            print("  ", sqlquery)
        paramNames,paramValueList,paramTypeList=self.execute_request(sqlquery)
        if verbose:
            print("INFO: %i rows found for this query" % len(paramValueList))

        # The requests returned no values
        if len(paramNames)+len(paramValueList)+len(paramTypeList)==0:
            return None

        # Build astropy tables
        columns_name = np.array(paramNames)
        columns_value = np.array(paramValueList)
        if len(paramTypeList)==0:
            result = Table(columns_value, names=columns_name)
        else:
            columns_dtype = np.array(paramTypeList)
            result = Table(columns_value, names=columns_name, dtype=columns_dtype)

        if save:
            query = {"sqlquery": sqlquery, "output": result}
            self.queries[len(self.queries) + 1] = query

        return result

    def _check_table(self, table):
        tables = self.get_all_tables()
        if table not in tables[tables.colnames[0]]:
            raise KeyError("%s in not in the available list of tables (see `get_all_tables`)")

    def get_all_tables(self, **kwargs):
        if self.tables is None:
            self.tables = self.query("SHOW TABLES", **kwargs)
        return self.tables

    def describe_table(self, table, **kwargs):
        self._check_table(table)
        return self.query("DESCRIBE %s" % table, **kwargs)

    def key_in_table(self, key, table):
        d = self.describe_table(table)
        return key in d['Field']

    def get_from_table(self, what, table, **kwargs):
        self._check_table(table)
        return self.query("SELECT %s from %s" % (what, table), **kwargs)


class QservQueryCatalogs(QservQuery):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)

    def complex_query(self):
        """TBF"""
        qq = {'deepCoadd_forced_src': {'and': ['modelfit_CModel_flux>0',
                                               'modelfit_CModel_flag=0'],
                                       'keys': ['modelfit_CModel_flux'],
                                       'or': []},
              'deepCoadd_meas': {'and': ['base_ClassificationExtendedness_flag=0',
                                         'base_ClassificationExtendedness_value>=0.5',
                                         'ext_shapeHSM_HsmShapeRegauss_flag=0',
                                         'detect_isPrimary=1'],
                                 'keys': ['ext_shapeHSM_HsmShapeRegauss_flag'],
                                 'or': []},
              'filter': {'keys': ['filter']}}
        what = ', '.join(['%s.%s' % (t, k) for t in qq for k in qq[t]['keys']])
        fromt = ', '.join(qq.keys())
        where_and = ' AND '
        where_and += ' AND '.join(['%s.%s' % (t, k) for t in qq if 'and' in qq[t] for k in qq[t]['and']])
        where_or = ' OR '.join(['%s.%s' % (t, k) for t in qq if 'or' in qq[t] for k in qq[t]['or']])
        
    def request(self,request):
        
        res=self.query(request)
        if res: print(res.info)
        print(res)
        
    def select_galaxies(self):
        """Apply a few quality filters on the data tables."""
        # == Get the initial number of filter
#        filters = self.query("SELECT distinct filter FROM deepCoadd_meas;")
#        print(filters)
#        nfilt = len(filters)

#        query = "SELECT [id, detect_isPrimary] FROM deepCoadd_meas as dm"
#        tab_meas=self.query(query, verbose=True)
#        print(tab_meas)


        query = "SELECT dm.id,dm.filter,dm.modelfit_CModel_mag,fs.modelfit_CModel_mag "
        query += "FROM deepCoadd_meas as dm,"
        query += "deepCoadd_forced_src as fs " 
        query += "WHERE dm.id=fs.objectId and dm.filter='r'"
        tab_meas=self.query(query, verbose=True)
        print(tab_meas.info)
        print(tab_meas)
        return tab_meas

        # == Filter the deepCoadd catalogs

        query = "SELECT [id,filter,*_flux,*_mag,*_magSigma] FROM deepCoadd_meas as dm WHERE "
        query += "AND dm.base_ClassificationExtendedness_flag=0 "
        query += "AND dm.base_ClassificationExtendedness_value >= 0.5 "
        query += "AND dm.ext_shapeHSM_HsmShapeRegauss_flag=0 "
        query += "AND dm.detect_isPrimary=1 "
        query += "AND dm.filter='g' "
#        query += "LIMIT 1000 "

        tab_meas=self.query(query, verbose=True)
        print(tab_meas)


        query = "SELECT [objectId,filter,*_flux,*_mag,*_magSigma] FROM deepCoadd_forced_src AS dfs WHERE "
        query += "AND dfs.modelfit_CModel_flux>0 "
        query += "AND dfs.modelfit_CModel_flag=0 "
        query += "AND (dfs.modelfit_CModel_flux/dfs.modelfit_CModel_fluxSigma)>10 "
        query += "AND dfs.filter='g' "
#        query += "LIMIT 1000 "

        tab_forced_src=self.query(query, verbose=True)
        print(tab_forced_src)


        tab_forced_src.rename_column("objectId","id")
        tab_all= join(tab_meas,tab_forced_src,keys=("id","filter"))

        
        print(tab_all.info)
        print(tab_all)

        return tab_all

