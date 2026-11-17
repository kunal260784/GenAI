from pyspark.sql.functions import col

class EnvConf:
    """Environment configuration holder.

    Class-level store for the spark session, env name, catalog, and control-table
    schema; loads env properties from the env control table for the active env.
    """

    spark = None
    env = None
    catalog = None
    controlTableSchema = None

    @classmethod
    def initialize(cls,spark, env: str, catalog: str, controlTableSchema : str):
        """Classmethod: store the spark session, env, catalog, and control-table schema
        on the class for later static access."""

        cls.spark = spark
        cls.env = env
        cls.catalog = catalog 
        cls.controlTableSchema = controlTableSchema

    @staticmethod
    def getEnvProperties():
        """Return env properties (propertyname -> propertyvalue) for the active env,
        read from the env control table."""

        return {item.propertyname : item.propertyvalue for item in (
                            EnvConf.spark.read.table(f"{EnvConf.catalog}.{EnvConf.controlTableSchema}.env")
                            .filter(col('name') == EnvConf.env)
                            .select('propertyname','propertyvalue')
                            .collect()
            )
        }


class NotebookConf:
    """Notebook configuration holder.

    Class-level store for notebook/source control-table context; resolves the
    notebook id and its properties from the control tables for a given source.
    """

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
        """Classmethod: store the spark session and notebook/source control-table
        context on the class for later static access."""

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
        """Return the notebook name stored at initialize time."""
        return NotebookConf.notebookName
    
    @staticmethod
    def getSourceName():
        """Return the source name stored at initialize time."""
        return NotebookConf.sourceName

    @staticmethod
    def getNotebookId():
        """Return the notebook's id by looking up its name in the notebook control table."""
        return (NotebookConf.spark.read.table(f"{NotebookConf.catalog}.{NotebookConf.controlTableSchema}.{NotebookConf.NotebookControlTable}")
                .filter(col('notebookname') == NotebookConf.notebookName)
                .select('id')
                .collect()[0][0])
        
    @staticmethod
    def getSourceId():
        """Return the source's id by looking up its name in the source control table."""
        # print(f"{NotebookConf.catalog}.{NotebookConf.controlTableSchema}.{NotebookConf.sourceControlTable}")
        return (NotebookConf.spark.read.table(f"{NotebookConf.catalog}.{NotebookConf.controlTableSchema}.{NotebookConf.sourceControlTable}")
                .filter(col('name') == NotebookConf.sourceName)
                .select('id')
                .collect()[0][0])

    @staticmethod
    def getNotebookProperties():
        """Return the notebook's properties (name -> value) from the notebook
        properties table, filtered by notebookid and sourceid."""

        return {item.name : item.value for item in 
                (NotebookConf.spark.read.table(f"{NotebookConf.catalog}.{NotebookConf.controlTableSchema}.{NotebookConf.NotebookPropertiesTable}")
                    .filter((col('notebookid') == NotebookConf.getNotebookId()) 
                            & (col('sourceid') == NotebookConf.getSourceId()))
                    .collect()
                )
                }

