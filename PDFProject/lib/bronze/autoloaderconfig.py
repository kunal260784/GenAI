from pyspark.sql.functions import col
    
class AutoLoaderInitialize:
    
        def __init__(self,spark,catalog,controlTableSchema,autoloaderPropertiesTable,sourceId):
            self.spark = spark
            self.catalog = catalog
            self.controlTableSchema = controlTableSchema
            self.autoloaderPropertiesTable = autoloaderPropertiesTable
            self.sourceId = sourceId 
    
        def getAutoloaderConfigProperties(self):

            return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.autoloaderPropertiesTable}")
                                        .filter((col("sourceid") == self.sourceId) & (col("type") == "config"))
                                        .select("id","name","value").orderBy("id").collect())
                    }   

        def getAutoloaderFileMetaProperties(self):

            return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.autoloaderPropertiesTable}")
                                        .filter((col("sourceid") == self.sourceId) & (col("type") == "meta"))
                                        .select("id","name","value").orderBy("id").collect())
                    } 

        def getAutoloaderTagetTableProperties(self):

            return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.autoloaderPropertiesTable}")
                                        .filter((col("sourceid") == self.sourceId) & (col("type") == "target"))
                                        .select("id","name","value").orderBy("id").collect())
                    }   

        def getAutoloaderTagetPartitionColumn(self):

            return {item.name : item.value  for item in (self.spark.read.table(f"{self.catalog}.{self.controlTableSchema}.{self.autoloaderPropertiesTable}")
                                        .filter((col("sourceid") == self.sourceId) & (col("type") == "targetPartition"))
                                        .select("id","name","value").orderBy("name").collect())
                    }         
        def checkSetup(self):
        
            print(f"AutoloaderConfigProperties = {self.getAutoloaderConfigProperties()}")
            print(f"AutoloaderTagetTableProperties = {self.getAutoloaderTagetTableProperties()}")
            print(f"AutoloaderTagetPartitionColumn = {self.getAutoloaderTagetPartitionColumn()}")
            print(f"AutoloaderFileMetaProperties = {self.getAutoloaderFileMetaProperties()}")
