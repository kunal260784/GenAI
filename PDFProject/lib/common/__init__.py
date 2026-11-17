"""Common utilities for ETL pipeline configuration.

This package provides configuration management for Databricks ETL pipelines:
- EnvConf: Environment-level configuration from Unity Catalog
- NotebookConf: Notebook-specific configuration and properties
- SparkProcessor: Orchestrates configuration loading and Spark session setup

Example usage:
    from lib.common import SparkProcessor
    
    processor = SparkProcessor(spark, notebook_name, env, catalog, schema)
    app_configs = processor.getApplicationConfigs()
"""

# Configuration classes
from .etlConfig import EnvConf, NotebookConf

# Processor class
from .sparkConfig import SourceInitialize

# Public API
__all__ = [
    'EnvConf',
    'NotebookConf',
    'SourceInitialize',
]

# Package metadata
__version__ = '1.0.0'
