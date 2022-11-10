import json
import os
"""
Author: Karunakar
url: github.com/karunakar2
"""

class kQGisAzureBulkImport:
    envVar = 'AZURE_STORAGE_CONNECTION_STRING'
    azConStr = os.getenv(envVar)
    def __init__(self): #not great place for code but keeping it simple
        self.logger = self._initLogging()
        self.logger.info('Thanks, Karunakar')

    def _initLogging(self):
        #from qgis.core import QgsProject
        #bPath = QgsProject.instance().homePath()
        if os.name == 'nt':
            bPath = os.path.expanduser(r'~\Documents')
        else:
            bPath = os.path.expanduser('~/Documents')
        bPath = os.path.join(bPath, "kQGisAzureBulkImport.log")
        import logging
        logging.basicConfig(filename=bPath, level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
        return logging.getLogger(__name__)

    def _progressDialog(self,progress):
        from qgis.PyQt.QtWidgets import QProgressDialog, QProgressBar
        dialog = QProgressDialog()
        dialog.setWindowTitle("kQGisAzureBulkImport")
        dialog.setLabelText("Downloading")
        bar = QProgressBar(dialog)
        bar.setTextVisible(True)
        bar.setValue(progress)
        dialog.setBar(bar)
        dialog.setMinimumWidth(300)
        dialog.show()
        return dialog, bar

        
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
            self.logger.error(er)
        return None

    def _thirdPartyModule(self,thisWheel):
        import sys
        import os
        this_dir = os.path.dirname(os.path.realpath(__file__))
        path = os.path.join(this_dir, thisWheel)
        sys.path.append(path)
    
    def _getAdlsFile(self,azContainer,azFilePath,fName):
        try:
            from azure.storage.filedatalake import DataLakeFileClient
        except ModuleNotFoundError:
            self._thirdPartyModule('azure_storage_file_datalake-12.0.0-py2.py3-none-any.whl')
        except ImportError:
            self._thirdPartyModule('azure_storage_file_datalake-12.0.0-py2.py3-none-any.whl')
        except:
            pass
        
        try:
            from azure.storage.filedatalake import DataLakeFileClient
        except Exception as er:
            self.logger.error(er)
            self._notifyThem("Please open 'OSGeo4W shell' (via windows menu) command prompt and")
            self._notifyThem("Execute 'python -m pip install azure-storage-file-datalake'")
            self._notifyThem("Wait for installation and grab a fresh start of QGIS for this plugin to work")
            
        file = DataLakeFileClient.from_connection_string(self.azConStr,
                                                         file_system_name=azContainer,
                                                         file_path=azFilePath)
        with open("./"+fName, 'wb') as my_file:
            download = file.download_file()
            file = None
            download.readinto(my_file)
        return fName

    def _load2Qgis(self,thisFile):
        from qgis.core import QgsVectorLayer, QgsProject
        vlayer = QgsVectorLayer(thisFile,thisFile.split('.')[0],"ogr")
        if not vlayer.isValid():
            self._notifyThem(str(thisFile)+" failed to load!")
        else:
            QgsProject.instance().addMapLayer(vlayer)

        myAttributes = vlayer.fields().names()
        if len(myAttributes) > 0:
            return myAttributes
        else:
            return None

    def _prepVirtLayer(self,myQuery,thisFile):
        self.logger.info(myQuery)
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
            self.logger.error(er)
            self._notifyThem('you should have got a copy of fetch (json) file or make one yourself')
            selectImportFile = None #intentional, next statement will break
            return None

        with open(selectImportFile,'r') as f:
            self._configFile = json.load(f)
            
        if self.azConStr is None:
            try:
                self.azConStr = self._configFile[self.envVar]
            except Exception as er:
                logging.error(er)
                
        if self.azConStr is None:
            from qgis.PyQt.QtWidgets import QInputDialog, QLineEdit
            thisText, ok = QInputDialog.getText(None, "Container customisation", "AZURE_STORAGE_CONNECTION_STRING:", QLineEdit.Normal, '')
            if ok and thisText:
                self.azConStr = thisText
            
        if self.azConStr is None: #still none?
            self._notifyThem('Please set '+str(envVar)+' in environment settings')
            return None #force fail the imported method

        #touch base with azure for the list of files
        self._getAdlsFile(self._configFile['container'], self._configFile['configFile'],'stream.temp')

        with open('stream.temp','rb') as f:
            workFile=json.load(f)

        barNumerator = 0
        progDialog, progBar = self._progressDialog(barNumerator)
        progBar.setValue(barNumerator)
        progBar.setMaximum(100)
        barDenominator = len(workFile['fileList'].keys())

        knownGeometryColumnNames = ['Geomstr','geomstr','geomStr','Geom','geom','Geometry','geometry']
        for thisContainer in workFile['fileList'].keys():
            if thisContainer != 'containerName':
                barNumerator += 1
                
                #self._notifyThem('--importing from'+str(thisContainer)+'--')
                for localName,cloudPath in workFile['fileList'][thisContainer].items():
                    #self._notifyThem(localName)
                    try:
                        self._getAdlsFile(thisContainer,cloudPath,localName)
                    except Exception as er:
                        self._notifyThem('Failed to fetch '+str(cloudPath))
                        self.logger.error(er)
                    
                    myAttributes = None
                    try:
                        myAttributes = self._load2Qgis(localName)
                    except Exception as er:
                        self.logger.error(er)
                        self._notifyThem('Cant load file to qgis: '+str(localName))
                        pass

                    try:
                        selectGeomField = list(set(myAttributes)&set(knownGeometryColumnNames))
                        gCol = None
                        if len(selectGeomField) > 0:
                            gCol = selectGeomField.pop() #picking the first
                        if gCol is not None:
                            #myAttributes.remove(gCol)
                            queryVars = ','.join(myAttributes)
                            queryVars += ',ST_GeomFromText('+str(gCol)+')'
                            self._prepVirtLayer(queryVars,localName)
                    except Exception as er:
                        self.logger.error(er)
                    gCol = None

                    progBar.setValue(int(float(barNumerator)/float(barDenominator)*100))
        return None

        




