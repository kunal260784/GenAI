from pyspark.sql.functions import col
    
class AutoLoaderInitialize:
        """Loads Auto Loader configuration from the autoloader properties control table.

        Properties are grouped by row type: 'config' (cloudFiles reader options),
        'meta' (file-metadata select expressions), 'target' (target table properties),
        and 'targetPartition' (partition columns), each filtered by source id.
        """
    
        def __init__(self,spark,catalog,controlTableSchema,autoloaderPropertiesTable,sourceId):
            """Store the spark session and control-table coordinates used by the
            property getters."""
            self.spark = spark
            self.catalog = catalog
            self.controlTableSchema = controlTableSchema
            self.autoloaderPropertiesTable = autoloaderPropertiesTable
            self.sourceId = sourceId 
    
        def getAutoloaderConfigProperties(self):
            """Return Auto Loader reader options (name -> value) where type == 'config'."""

            return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.autoloaderPropertiesTable}")
                                        .filter((col("sourceid") == self.sourceId) & (col("type") == "config"))
                                        .select("id","name","value").orderBy("id").collect())
                    }   

        def getAutoloaderFileMetaProperties(self):
            """Return file-metadata select expressions (name -> value) where type == 'meta'."""

            return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.autoloaderPropertiesTable}")
                                        .filter((col("sourceid") == self.sourceId) & (col("type") == "meta"))
                                        .select("id","name","value").orderBy("id").collect())
                    } 

        def getAutoloaderTagetTableProperties(self):
            """Return target table properties (name -> value) where type == 'target'."""

            return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.autoloaderPropertiesTable}")
                                        .filter((col("sourceid") == self.sourceId) & (col("type") == "target"))
                                        .select("id","name","value").orderBy("id").collect())
                    }   

        def getAutoloaderTagetPartitionColumn(self):
            """Return target partition columns (name -> value) where type == 'targetPartition'."""

            return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.autoloaderPropertiesTable}")
                                        .filter((col("sourceid") == self.sourceId) & (col("type") == "targetPartition"))
                                        .select("id","name","value").orderBy("name").collect())
                    }         
        def checkSetup(self):
            """Print all resolved Auto Loader property groups for debugging/verification."""
        
            print(f"AutoloaderConfigProperties = {self.getAutoloaderConfigProperties()}")
            print(f"AutoloaderTagetTableProperties = {self.getAutoloaderTagetTableProperties()}")
            print(f"AutoloaderTagetPartitionColumn = {self.getAutoloaderTagetPartitionColumn()}")
            print(f"AutoloaderFileMetaProperties = {self.getAutoloaderFileMetaProperties()}")
