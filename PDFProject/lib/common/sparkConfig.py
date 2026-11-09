from pyspark.sql.functions import col, current_timestamp, current_user
from pyspark.sql.types import StructType, StructField, StringType, LongType

from lib.common.etlConfig import EnvConf, NotebookConf
from datetime import datetime

class SourceInitialize:

    def __init__(self,spark,nb_name, env, catalog, controlTableSchema, source):
        
        self.env = env
        self.catalog = catalog
        self.controlTableSchema = controlTableSchema
        self.nb_name = nb_name
        self.spark = spark 
        self.source = source    
          
        EnvConf.initialize(self.spark, self.env, self.catalog, self.controlTableSchema)  
        self.envConfig = EnvConf.getEnvProperties()

        self.notebookControlTable = self.envConfig.get("notebookControlTable")        
        self.notebookPropertiesTable = self.envConfig.get("notebookPropertiesTable")
        self.sourceControlTable = self.envConfig.get("sourceTable")  
        self.autoloaderPropertiesTable = self.envConfig.get("autoloaderPropertiesTable")
        self.sourcePropertiesTable = self.envConfig.get("sourcePropertiesTable")  
        self.rejectedRecordsTable = self.envConfig.get("rejectedRecordsTable")  
        NotebookConf.initialize(self.spark, self.nb_name,self.env, self.catalog, self.controlTableSchema, self.notebookControlTable, self.notebookPropertiesTable, self.sourceControlTable, self.source)
        self.notebookName = NotebookConf.getNotebookName()
        self.notebookId = NotebookConf.getNotebookId()
        self.notebookProperties = NotebookConf.getNotebookProperties()
        self.spark = self.setSparkConfigs()
        self.sourceName = NotebookConf.getSourceName()
        self.sourceId = NotebookConf.getSourceId()

    def getRejectedRecordsTable(self):
        return self.rejectedRecordsTable
        
    def getAutoloaderPropertiesTable(self):
        return self.autoloaderPropertiesTable
    
    def getSourcePropertiesTable(self):
        return self.sourcePropertiesTable      
    
    def getNotebookName(self):
        return self.notebookName

    def getNotebookId(self):
        return self.notebookId

    def getNotebookControlTable(self):
        return self.notebookControlTable

    def getNotebookPropertiesTable(self):
        return self.notebookPropertiesTable
    
    def getSourceControlTable(self):
        return self.sourceControlTable
    
    def getSourceId(self):
        return self.sourceId
    
    def getSourceName(self):
        return self.sourceName
    
    def getNotebookProperties(self):
        return self.notebookProperties
    
    def getLoadId(self):
        return datetime.now().strftime("%Y%m%d%H%M%S")

    def setSparkConfigs(self):  
        
        for key,val in self.notebookProperties.items() :
            if key.startswith("spark."):
                self.spark.conf.set(key, val)

        return self.spark

    def getApplicationConfigs(self):
        
        return { key:val for key,val in self.notebookProperties.items() if not key.startswith("spark.")}   

    def getSourceProperties(self):

                return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.sourcePropertiesTable}")
                                    .filter(col("sourceid") == self.sourceId)
                                    .select("name","value").collect())
                }

    def auditInsert(self, loadid, batch_id, files_processed, targetCatelog, targetSchema, layer):

                audit_records = [(
                                int(loadid),
                                int(batch_id),
                                self.sourceName,
                                int(files_processed),
                                "SUCCESS",
                                None
                                )]

                audit_schema = StructType([
                    StructField("loadid", LongType(), False),
                    StructField("batchid", LongType(), False),
                    StructField("sourcename", StringType(), False),
                    StructField("files_processed", LongType(), False),
                    StructField("status", StringType(), False),
                    StructField("error_message", StringType(), True)
                ])
                
                audit_df = (
                                self.spark.createDataFrame(
                                        audit_records, 
                                        schema=audit_schema
                                )
                                .select("*",current_timestamp().alias("loaddt" ), current_user().alias("createdby"))
                        )
                

                (audit_df.write.format("delta")
                                .mode("append")
                                .option("mergeSchema", "true")
                                .saveAsTable(f'{targetCatelog}.{targetSchema}.{layer}_auditlog')
                )

    def checkSetup(self):

        print(f"Notebook Name = {self.getNotebookName()}")
        print(f"Notebook ID = {self.getNotebookId()}")
        print(f"NotebookControlTable = {self.getNotebookControlTable()}")
        print(f"getNotebookPropertiesTable = {self.getNotebookPropertiesTable()}")
        print(f"SourceControlTable = {self.getSourceControlTable()}")
        print(f"SourceName = {self.getSourceName()}")
        print(f"SourceId = {self.getSourceId()}")
        print(f"ApplicationConfig = {self.getApplicationConfigs()}")
        print(f"AutoloaderPropertiesTable = {self.getAutoloaderPropertiesTable()}")
        print(f"SourcePropertiesTable = {self.getSourcePropertiesTable()}")
        print(f"SourceProperties = {self.getSourceProperties()}")

