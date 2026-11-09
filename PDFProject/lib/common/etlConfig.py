from pyspark.sql.functions import col

class EnvConf:

    spark = None
    env = None
    catalog = None
    controlTableSchema = None

    @classmethod
    def initialize(cls,spark, env: str, catalog: str, controlTableSchema : str):

        cls.spark = spark
        cls.env = env
        cls.catalog = catalog 
        cls.controlTableSchema = controlTableSchema

    @staticmethod
    def getEnvProperties():

        return {item.propertyname : item.propertyvalue for item in (
                            EnvConf.spark.read.table(f"{EnvConf.catalog}.{EnvConf.controlTableSchema}.env")
                            .filter(col('name') == EnvConf.env)
                            .select('propertyname','propertyvalue')
                            .collect()
            )
        }


class NotebookConf:

    notebookName = None
    spark = None
    env = None
    catalog = None
    controlTableSchema = None
    NotebookControlTable = None
    NotebookPropertiesTable = None
    sourceName = None
    sourceControlTable = None

    @classmethod
    def initialize(cls,spark, notebookName: str, env: str, catalog: str, controlTableSchema : str, NotebookControlTable:str, NotebookPropertiesTable:str,  sourceControlTable:str, sourceName: str):

        cls.spark = spark
        cls.notebookName = notebookName
        cls.env = env
        cls.catalog = catalog 
        cls.controlTableSchema = controlTableSchema
        cls.NotebookControlTable = NotebookControlTable
        cls.NotebookPropertiesTable = NotebookPropertiesTable
        cls.sourceControlTable = sourceControlTable
        cls.sourceName = sourceName

    @staticmethod
    def getNotebookName():
        return NotebookConf.notebookName
    
    @staticmethod
    def getSourceName():
        return NotebookConf.sourceName

    @staticmethod
    def getNotebookId():
        return (NotebookConf.spark.read.table(f"{NotebookConf.catalog}.{NotebookConf.controlTableSchema}.{NotebookConf.NotebookControlTable}")
                .filter(col('notebookname') == NotebookConf.notebookName)
                .select('id')
                .collect()[0][0])
        
    @staticmethod
    def getSourceId():
        # print(f"{NotebookConf.catalog}.{NotebookConf.controlTableSchema}.{NotebookConf.sourceControlTable}")
        return (NotebookConf.spark.read.table(f"{NotebookConf.catalog}.{NotebookConf.controlTableSchema}.{NotebookConf.sourceControlTable}")
                .filter(col('name') == NotebookConf.sourceName)
                .select('id')
                .collect()[0][0])

    @staticmethod
    def getNotebookProperties():

        return {item.name : item.value for item in 
                (NotebookConf.spark.read.table(f"{NotebookConf.catalog}.{NotebookConf.controlTableSchema}.{NotebookConf.NotebookPropertiesTable}")
                    .filter((col('notebookid') == NotebookConf.getNotebookId()) 
                            & (col('sourceid') == NotebookConf.getSourceId()))
                    .collect()
                )
                }

