This plugin helps anyone trying to connect to Azure containers to fetch the files.
While the ogr has standard method to grab the files one by one, described in below link
https://gis.stackexchange.com/questions/414343/connecting-qgis-with-azure-adls-gen2-storage
This plugin is targeted at importing a bulk of files with a configuration file.

The current implementation needs two files, which are json built.
1. A local configFile (which points to the another on the cloud) formatted as below
{
    "container":",container",
    "configFile":"mainDir/subDir/fullConfig.json",
	    "AZURE_STORAGE_CONNECTION_STRING":""
}
the file name can be anything with json extension (preferred)

2. The second json file sitting on the cloud has actual paths and how the local files should be named
{
    "author":"karunakar",
    "date":"8/11/22",
    "template":"github.com/karunakar2",
    "fileList":{
        "containerName":{
            "localFileName":"cloudFilePath"
            }
        }
    }
    please dont use abfss paths, but use the relative paths here.
    This file is named "mainDir/subDir/fullConfig.json" on cloud.
    
    Enjoy, its still a dev release but working on functionality.
    
    
