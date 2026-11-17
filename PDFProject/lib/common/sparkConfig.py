from pyspark.sql.functions import col, current_timestamp, current_user
from pyspark.sql.types import StructType, StructField, StringType, LongType

from lib.common.etlConfig import EnvConf, NotebookConf
from datetime import datetime

class SourceInitialize:
    """Central initializer for a source's control-table configuration.

    Resolves env properties, notebook id/properties, and source id/name from the
    control tables, and applies spark.* notebook properties to the Spark session.
    """

    def __init__(self,spark,nb_name, env, catalog, controlTableSchema, source):
        """Initialize environment, notebook, and source configuration.

        Args:
            spark: active SparkSession.
            nb_name: notebook name (used to look up the notebook in the control tables).
            env: environment name (e.g. 'dev') to read properties for.
            catalog: Unity Catalog name holding the control tables.
            controlTableSchema: schema holding the control tables.
            source: source name to resolve the source id from.

        Reads env properties, initializes NotebookConf, applies spark.* properties
        to the session, and resolves the source name/id.
        """
        
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
        self.bronzetableproperties = self.envConfig.get("bronzetableproperties") 
        NotebookConf.initialize(self.spark, self.nb_name,self.env, self.catalog, self.controlTableSchema, self.notebookControlTable, self.notebookPropertiesTable, self.sourceControlTable, self.source)
        self.notebookName = NotebookConf.getNotebookName()
        self.notebookId = NotebookConf.getNotebookId()
        self.notebookProperties = NotebookConf.getNotebookProperties()
        self.spark = self.setSparkConfigs()
        self.sourceName = NotebookConf.getSourceName()
        self.sourceId = NotebookConf.getSourceId()

    def getRejectedRecordsTable(self):
        """Return the configured rejected-records table name."""
        return self.rejectedRecordsTable
        
    def getAutoloaderPropertiesTable(self):
        """Return the configured Auto Loader properties table name."""
        return self.autoloaderPropertiesTable
    
    def getSourcePropertiesTable(self):
        """Return the configured source properties table name."""
        return self.sourcePropertiesTable      
    
    def getNotebookName(self):
        """Return the resolved notebook name."""
        return self.notebookName

    def getNotebookId(self):
        """Return the resolved notebook id."""
        return self.notebookId

    def getNotebookControlTable(self):
        """Return the configured notebook control table name."""
        return self.notebookControlTable

    def getNotebookPropertiesTable(self):
        """Return the configured notebook properties table name."""
        return self.notebookPropertiesTable
    
    def getSourceControlTable(self):
        """Return the configured source control table name."""
        return self.sourceControlTable
    
    def getSourceId(self):
        """Return the resolved source id."""
        return self.sourceId
    
    def getSourceName(self):
        """Return the resolved source name."""
        return self.sourceName
    
    def getNotebookProperties(self):
        """Return the notebook's properties as a dict (name -> value)."""
        return self.notebookProperties
    
    def getLoadId(self):
        """Return a load id: the current timestamp formatted as YYYYMMDDHHMMSS."""
        return datetime.now().strftime("%Y%m%d%H%M%S")

    def setSparkConfigs(self):  
        """Apply all notebook properties starting with 'spark.' to the Spark session
        and return the session."""
        
        for key,val in self.notebookProperties.items() :
            if key.startswith("spark."):
                self.spark.conf.set(key, val)

        return self.spark

    def getApplicationConfigs(self):
        """Return notebook properties that are not spark.* keys (application-level config)."""
        
        return { key:val for key,val in self.notebookProperties.items() if not key.startswith("spark.")}   

    def getSourceProperties(self):
        """Return source properties (name -> value) from the source properties table,
        filtered by this source id."""

        return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.sourcePropertiesTable}")
                            .filter(col("sourceid") == self.sourceId)
                            .select("name","value").collect())
        }

    def getBronzeProperties(self):
        """Return bronze table write properties (name -> value) from the bronze table
        properties table, filtered by this source id."""

        return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.bronzetableproperties}")
                            .filter(col("sourceid") == self.sourceId)
                            .select("name","value").collect())
        }

    # def auditInsert(self, loadid, batch_id, files_processed, targetCatelog, targetSchema, layer):

    #             audit_records = [(
    #                             int(loadid),
    #                             int(batch_id),
    #                             self.sourceName,
    #                             int(files_processed),
    #                             "SUCCESS",
    #                             None
    #                             )]

    #             audit_schema = StructType([
    #                 StructField("loadid", LongType(), False),
    #                 StructField("batchid", LongType(), False),
    #                 StructField("sourcename", StringType(), False),
    #                 StructField("files_processed", LongType(), False),
    #                 StructField("status", StringType(), False),
    #                 StructField("error_message", StringType(), True)
    #             ])
                
    #             audit_df = (
    #                             self.spark.createDataFrame(
    #                                     audit_records, 
    #                                     schema=audit_schema
    #                             )
    #                             .select("*",current_timestamp().alias("loaddt" ), current_user().alias("createdby"))
    #                     )
                

    #             (audit_df.write.format("delta")
    #                             .mode("append")
    #                             .option("mergeSchema", "true")
    #                             .saveAsTable(f'{targetCatelog}.{targetSchema}.{layer}_auditlog')
    #             )

    def checkSetup(self):
        """Print all resolved configuration values for debugging/verification."""

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
        print(f"BronzeTableProperties = {self.getBronzeProperties()}")

