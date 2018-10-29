
import sys

from qserv_query import *

from drptools.data import DRPCatalogs
from drptools.data import DRPLoader
from drptools.data import QservNameConverter

import argparse
import random

import pprint

## config = {"keys": {'deepCoadd_meas': ["coord*", "id", 'base_ClassificationExtendedness_flag',
##                                       'base_ClassificationExtendedness_value',
##                                       'modelfit_CModel_flux', 'modelfit_CModel_fluxSigma',
##                                       'modelfit_CModel_flag',
##                                       'ext_shapeHSM_HsmShapeRegauss_flag',
##                                       'ext_shapeHSM_HsmShapeRegauss_e1',
##                                       'ext_shapeHSM_HsmShapeRegauss_e2',
##                                       'ext_shapeHSM_HsmShapeRegauss_sigma',
##                                       'ext_shapeHSM_HsmShapeRegauss_resolution',
##                                       'ext_shapeHSM_HsmPsfMoments_flag',
##                                       'ext_shapeHSM_HsmPsfMoments_x',
##                                       'ext_shapeHSM_HsmPsfMoments_y',
##                                       'ext_shapeHSM_HsmPsfMoments_xx',
##                                       'ext_shapeHSM_HsmPsfMoments_xy',
##                                       'ext_shapeHSM_HsmPsfMoments_yy',
##                                       'ext_shapeHSM_HsmSourceMoments_flag',
##                                       'ext_shapeHSM_HsmSourceMoments_x',
##                                       'ext_shapeHSM_HsmSourceMoments_y',
##                                       'ext_shapeHSM_HsmSourceMoments_xx',
##                                       'ext_shapeHSM_HsmSourceMoments_xy',
##                                       'ext_shapeHSM_HsmSourceMoments_yy',
##                                       'base_CircularApertureFlux_9_0*',
##                                       'base_PsfFlux_flag', 'base_PsfFlux_flux',
##                                       'base_PsfFlux_fluxSigma',
##                                       'x_Src', 'y_Src', 'detect_isPrimary'],
##                    'deepCoadd_forced_src': ["coord*", "objectId", 'modelfit_CModel_flux',
##                                             'modelfit_CModel_flag', "modelfit_CModel*"]
##                },
##           "patch": ['1,1', '1,3', '1,2', '1,4', '1,5'] 
##           }


config = {"keys": {'deepCoadd_meas': ["*"],
                   'deepCoadd_forced_src': ["*"],
                   },
          }


def DumpParamConversionDict(dataPath,fileId):

    drp = DRPLoader(dataPath)
    drp.overview()

    catalogNames=list(config["keys"].keys())
    print(catalogNames)

    local_config=config.copy()
    keyList=["tract","patch","filter"]
    for k in keyList: 
        if k in local_config: del local_config[k]
    local_config["oneIdOnly"]=True

    print("-> get catalogs from drptools - ")
    catOneId=DRPCatalogs(dataPath,['raw','deepCoadd_meas', 'deepCoadd_forced_src'])    
    catOneId.load_catalogs(catalogNames,**local_config)
    print(">> DefineDbStructureBasedOnCatalog - DONE ")
    
    print("catOneId : deepCoadd_meas -> #params : ",len(catOneId.catalogs["deepCoadd_meas"].keys()))
    print("catOneId : deepCoadd_forced_src -> #params: ",len(catOneId.catalogs["deepCoadd_forced_src"].keys()))

    qservNameConverter=QservNameConverter()
    for catName in catalogNames:
        qservNameConverter.build_qserv_shortenNames(catName,catOneId.catalogs[catName].keys())        

    f=open("qservNameConversion_%s.py"%(fileId),"w")
    tmp1=pprint.pformat(qservNameConverter.get_qserv_shortenNames("deepCoadd_meas"))
    tmp2=pprint.pformat(qservNameConverter.get_qserv_shortenNames("deepCoadd_forced_src"))
    f.write("paramNameConvDict ={\n")
    f.write('"deepCoadd_meas" :'+tmp1+',\n')
    f.write('"deepCoadd_forced_src":'+tmp2+',\n')
    f.write("}\n")
    f.close()

    return



if __name__=="__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--conv", help="create parameter name conversion file", action="store_true")
    parser.add_argument("--dataPath", type=str, help="directory that contains the data ")
    parser.add_argument("--convFileId", type=str, help="parameter name conversion file name")
    parser.add_argument("--db", type=str, help="database name")

    parser.add_argument("--showdatabases", help="display list of databases", action="store_true")
    parser.add_argument("--showtables", help="display list of tables", action="store_true")
    parser.add_argument("--describetable", type=str, help="describe tables")
    parser.add_argument("--request", type=str, help="request")
    parser.add_argument("--galaxies",  help="select_galaxies example", action="store_true")

    args = parser.parse_args()
    print(args)

    convFileId_setup=args.db
    if args.convFileId: convFileId_setup=args.convFileId

    if args.conv:
        if not args.dataPath:
            print("No data path defined (use --dataPath)")
            sys.exit()
        DumpParamConversionDict(args.dataPath,convFileId_setup)
        sys.exit()

    qservQueryCat=QservQueryCatalogs(dbName=args.db,convFileId=convFileId_setup,user="qsmaster", host="ccqserv125.in2p3.fr", port=30040)

    if args.showdatabases:
        qservQueryCat.request("SHOW DATABASES;")
        sys.exit()

    if args.showtables:
        qservQueryCat.request("SHOW TABLES;")
        sys.exit()

    if args.describetable:
        qservQueryCat.request("DESCRIBE %s;"%args.describetable)
        sys.exit()

    if args.request:
        qservQueryCat.request(args.request)
        sys.exit()
        
    if args.galaxies:
        qservQueryCat.select_galaxies()

