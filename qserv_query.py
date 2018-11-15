"""
Some doc https://mysqlclient.readthedocs.io/index.html
"""

import sys
import subprocess
import re
import numpy as np
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


    def replace_select_from_wildcards(self,request):

        # Extract parameter list and db name
        tmp_low=[x.lower() for x in request.split(" ") if x!=""]
        try:
            iSelect=tmp_low.index("select")
        except:
            return request,None
        if tmp_low[iSelect+1]=="distinct": iSelect+=1
        iTableName=tmp_low.index("from")

        tmp_request=[x for x in request.split(" ") if x!=""]
        paramNames=tmp_request[iSelect+1]
        tableName=tmp_request[iTableName+1]
        if tableName.endswith(";"): tableName=tableName[:-1]
        if not paramNames[0]=='[': return request,tableName

        # Define parameter name pattern list 
        patternList=[x for x in paramNames[1:-1].split(",") if x!=""]

        # Get parameter names corresponding to the patterns defined above
        paramList=[]
        for p in patternList:
            p=p.replace("*","\w+")
            if not p[-1]=="*": p=p+"$"
            tmp=[x for x in self.paramNameConvDict[tableName] if re.match(p,x)]
            paramList.extend(tmp)

        # Update db request
        paramList=list(set(paramList))
        paramNames=",".join(paramList)
        paramNames=paramNames.replace("'","")
        tmp_request[iSelect+1]=paramNames

        return " ".join(tmp_request),tableName


    def replace_select_by_shortnames(self,input_request,tableName):

        tmp_low=[x.lower() for x in input_request.split(" ") if x!=""]
        try:
            iSelect=tmp_low.index("select")
        except:
            return input_request
        
        # replaces parameter names by shorten parameter names starting
        #       with the longest parameter name
        for k in sorted(self.paramNameConvDict[tableName], key=len, reverse=True):
            input_request=input_request.replace(k,self.paramNameConvDict[tableName][k])

        return input_request


    def execute_request(self,request):

        # Check if select --- from requets section contains wildcards
        request_init=request
        tableName=None
        
        if self.paramNameConvDict:        
            request,tableName=self.replace_select_from_wildcards(request_init)
            print("wildcards : ",request)
            print("table name : ",tableName)

            # Replace parameter names by shorten names
            request=self.replace_select_by_shortnames(request,tableName)
            print("short names : ",request)
            
        if not request.endswith(";"): request+=";"

        # Final request
        if self.dbName: 
            cmd='%s %s -e "%s"'%(self.mysqlCommand,self.dbName,request)
        else:
            cmd='%s -e "%s"'%(self.mysqlCommand,request)
            
        cmd=cmd.replace("WHERE AND","WHERE")
        print(cmd)
        p=subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        res,err=p.communicate()
        if err: print("Error : ",err)

        exitcode=p.returncode
        if exitcode!=0: print(res,"\nExitCode : ",exitcode)
        if exitcode!=0: return

        # Get data types
        dataTypeConverter={}
        if tableName: 
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
                    dataTypeConverter[tmp[0]]=bool
                elif "float" in dType or "double" in dType:
                    dataTypeConverter[tmp[0]]=float
                elif "int" in dType:
                    dataTypeConverter[tmp[0]]=int
                elif "char" in dType:
                    dataTypeConverter[tmp[0]]=str
                else:
                    print("UNDEFINED mysl data type : ",name," ",dType)
                    sys.exit()
                    

        # Decode request output
        resTmp=res.decode("ascii")

        # Extract parameter names and row values
        lines=[x for x in resTmp.split("\n") if x!=""]
        paramNames=[x for x in lines[0].split("\t") if x!=""]
        paramValueList=[]
        for i,l in enumerate(lines[1:]):
            if dataTypeConverter:
                vList=[]
                for j,x in enumerate(l.split("\t")):
                    if paramNames[j] in dataTypeConverter : vList.append(dataTypeConverter[paramNames[j]](x.strip()))
                    else: vList.append(x.strip())

            else:
                vList=[x.strip() for x in l.split("\t")]
            paramValueList.append(tuple(vList))
#            if i==0:
#                for v in vList: print(v," ",type(v))

        # Replace shorten parameter names by real names
        paramNames_real=[]
        if self.paramNameConvDict and tableName:
            inv_names = {v: k for k, v in self.paramNameConvDict[tableName].items()}        
            for p in paramNames:
                if p in inv_names:
                    paramNames_real.append(inv_names[p])
                else:
                    paramNames_real.append(p)
        else:
            paramNames_real=paramNames

        
        paramTypeList=[]
        for j,x in enumerate(paramNames):
            if x in dataTypeConverter : paramTypeList.append(dataTypeConverter[x].__name__)
        
#       print(paramNames)
#       print(paramNames_real)
#       print(paramTypeList)

        return paramNames_real,paramValueList,paramTypeList


    def query(self, sqlquery, save=True, verbose=False):

        if verbose:
            print("Current query is")
            print("  ", sqlquery)
        paramNames,paramValueList,paramTypeList=self.execute_request(sqlquery)
        if verbose:
            print("INFO: %i rows found for this query" % len(paramValueList))

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
        print(res.info)
        print(res)
        
    def select_galaxies(self):
        """Apply a few quality filters on the data tables."""
        # == Get the initial number of filter
#        filters = self.query("SELECT distinct filter FROM deepCoadd_meas;")
#        print(filters)
#        nfilt = len(filters)

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

        
##         query = "SELECT * FROM deepCoadd_meas AS dm, deepCoadd_forced_src AS dfs WHERE dm.id=dfs.objectId "
## #        query = "SELECT * FROM deepCoadd_meas AS dm, deepCoadd_forced_src AS dfs WHERE dm.deepCoadd_ref_fkId=dfs.deepCoadd_ref_fkId "
##         # Select galaxies (and reject stars)
##         # keep galaxy
##         query += "AND dm.base_ClassificationExtendedness_flag=0 "
##         #filt = cats['deepCoadd_meas']['base_ClassificationExtendedness_flag'] == 0

##         # keep galaxy
##         query += "AND dm.base_ClassificationExtendedness_value >= 0.5 "
##         #filt &= cats['deepCoadd_meas']['base_ClassificationExtendedness_value'] >= 0.

##         # Gauss regulerarization flag
##         query += "AND dm.ext_shapeHSM_HsmShapeRegauss_flag=0 "
##         #filt &= cats['deepCoadd_meas']['ext_shapeHSM_HsmShapeRegauss_flag'] == 0

##         # Make sure to keep primary sources
##         query += "AND dm.detect_isPrimary=1 "
##         #filt &= cats['deepCoadd_meas']['detect_isPrimary'] == 1

##         # Check the flux value, which must be > 0
##         query += "AND dfs.modelfit_CModel_flux>0 "
##         #filt &= cats['deepCoadd_forced_src']['modelfit_CModel_flux'] > 0

##         # Select sources which have a proper flux value
##         query += "AND dfs.modelfit_CModel_flag=0 "
##         #filt &= cats['deepCoadd_forced_src']['modelfit_CModel_flag'] == 0

##         # Check the signal to noise (stn) value, which must be > 10
##         query += "AND (dfs.modelfit_CModel_flux/dfs.modelfit_CModel_fluxSigma)>10 "
##         #filt &= (cats['deepCoadd_forced_src']['modelfit_CModel_flux'] /
##         #         cats['deepCoadd_forced_src']['modelfit_CModel_fluxSigma']) > 10


##        return res

 #
 ## == Only keeps sources with the 'nfilt' filters
 #dmg = cats['deepCoadd_meas'][filt].group_by('id')
 #dfg = cats['deepCoadd_forced_src'][filt].group_by(
 #    'id' if 'id' in cats['deepCoadd_forced_src'].keys() else 'objectId')
 #
 ## Indices difference is a quick way to get the lenght of each group
 #filt = (dmg.groups.indices[1:] - dmg.groups.indices[:-1]) == nfilt
 #
 #output = {'deepCoadd_meas': dmg.groups[filt],
 #          'deepCoadd_forced_src': dfg.groups[filt], 'wcs': cats['wcs']}
 #
 ## == Filter the forced_src catalog: only keep objects present in the other catalogs
 #if "forced_src" not in cats.keys():
 #    return output
 #
 #filt = np.where(np.in1d(cats['forced_src']['objectId'],
 #                        output['deepCoadd_meas']['id']))[0]
 #output['forced_src'] = cats['forced_src'][filt]
 #
 #return output
