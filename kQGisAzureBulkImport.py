import json
import os
"""
Author: Karunakar
url: github.com/karunakar2
"""

import logging
logging.basicConfig(filename='./kQGisAzureBulkImport.log', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)
    
class kQGisAzureBulkImport:
    envVar = 'AZURE_STORAGE_CONNECTION_STRING'
    azConStr = os.getenv(envVar)
    def __init__(self): #not great place for code but keeping it simple
        logger.info('Thanks, Karunakar')
        if self.azConStr is None:
            self._notifyThem('Please set '+str(envVar)+' in environment settings')

    def _notifyThem(self, myMessage:str):
        try:
            from qgis.PyQt.QtWidgets import QMessageBox
            msgBox = QMessageBox()
            msgBox.setText(myMessage)
            msgBox.exec()
        except Exception as er:
            from qgis.core import Qgis
            from qgis.utils import iface
            iface.messageBar().pushMessage("Err", myMessage, level=Qgis.Critical)
            print(myMessage)
            logger.error(er)
        return None
        
    def _getAdlsFile(self,azContainer,azFilePath,fName):
        try:
            from azure.storage.filedatalake import DataLakeFileClient
            file = DataLakeFileClient.from_connection_string(self.azConStr,
                                                         file_system_name=azContainer,
                                                         file_path=azFilePath)
        except Exception as er:
            logger.error(er)
            self._notifyThem("Please open 'OSGeo4W shell' (windows menu) and")
            self._notifyThem("Execute 'pythom -m pip install azure-storage-file-datalake'")
            self._notifyThem("Wait for installation and grab a fresh start of QGIS for this plugin to work")

        with open("./"+fName, 'wb') as my_file:
            download = file.download_file()
            file = None
            download.readinto(my_file)
        return fName

    def _load2Qgis(self,thisFile):
        from qgis.core import QgsVectorLayer, QgsProject
        vlayer = QgsVectorLayer(thisFile,thisFile.split('.')[0],"ogr")
        if not vlayer.isValid():
            self._notifyThem(str(localName)+" Layer failed to load!")
        else:
            QgsProject.instance().addMapLayer(vlayer)
        return None

    def _prepVirtLayer(self,myQuery,thisFile):
        logging.info(myQuery)
        if len(str(myQuery)) > 0:
            myLayer = str(thisFile.split('.')[0])
            myQuery = 'select '+myQuery
            myQuery += ' from '+myLayer
            from qgis.core import QgsVectorLayer, QgsProject
            vlayer = QgsVectorLayer( "?query="+myQuery, myLayer, "virtual")
            QgsProject.instance().addMapLayer(vlayer)
        else:
            raise Exception
        return None
            
    def run(self):
        try:
            from qgis.PyQt.QtWidgets import QFileDialog
            #Project directory, files come down here
            dirName = QFileDialog.getExistingDirectory(None,'Project Directory',"",)
            os.chdir(dirName)
            
            #The json file pointing to the file list config file
            #you should have got a copy or make one yourself
            dialog = QFileDialog()
            dialog.setFileMode(QFileDialog.ExistingFile)
            dialog.setNameFilter("*.json")
            selectImportFile = (dialog.getOpenFileName(None,'ConfigFile',"",))[0]
        except Exception as er:
            logger.error(er)
            self._notifyThem('you should have got a copy of json file point to file list json file on cloud or make one yourself')
            selectImportFile = None #intentional, next statements break
            
        with open(selectImportFile,'r') as f:
            configFile = json.load(f)
            self._getAdlsFile(configFile['container'], configFile['configFile'],'stream.temp')

        with open('stream.temp','rb') as f:
            workFile=json.load(f)

        for thisContainer in workFile['fileList'].keys():
            if thisContainer != 'containerName':
                #self._notifyThem('--importing from'+str(thisContainer)+'--')
                for localName,cloudPath in workFile['fileList'][thisContainer].items():
                    #self._notifyThem(localName)
                    try:
                        self._getAdlsFile(thisContainer,cloudPath,localName)
                    except Exception as er:
                        self._notifyThem('Failed to fetch '+str(cloudPath))
                        logger.error(er)
                    gCol = None
                    myAttributes = None
                    try:
                        if localName.split('.')[1] == 'parquet':
                            import pandas as pd
                            df = pd.read_parquet(localName)
                            myAttributes = df.columns.copy()
                            if 'Geomstr' in myAttributes:
                                gCol = 'Geomstr'
                            if 'Geom' in myAttributes:
                                gCol = 'Geom'
                            if 'Geometry' in myAttributes:
                                gCol = 'Geometry'
                            if gCol is not None:
                                import geopandas as gpd
                                gdf = gpd.GeoDataFrame(df,geometry=gpd.GeoSeries.from_wkt(df[gCol]))
                                gdf.to_parquet('g-'+localName)
                                localName = 'g-'+localName #wont get updated if above fails
                                gdf = None
                                
                    except Exception as er:
                        #self._notifyThem('Cant prepare geoparquet files for '+str(localName))
                        logger.error('Cant prepare geoparquet files for '+str(localName))
                        logger.error(er)

                    try:
                        self._load2Qgis(localName)
                    except Exception as er:
                        logger.error(er)
                        self._notifyThem('Cant load file to qgis: '+str(localName))
                        pass

                    if 'g-' not in localName:
                        try:
                            if gCol is not None:
                                #myAttributes.remove(gCol)
                                queryVars = ','.join(myAttributes)
                                queryVars += ',ST_GeomFromText('+str(gCol)+')'
                                self._prepVirtLayer(queryVars,localName)
                        except Exception as er:
                            logger.error(er)
                    gCol = None
        return None

        




