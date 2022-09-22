# Databricks notebook source
# MAGIC %pip install folium==0.12.1 polygon_geohasher==0.0.1 geopandas==0.9.0 python-geohash==0.8.5

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

from configparser import ConfigParser
config = ConfigParser()
config.read('config/application.ini')

# COMMAND ----------

db_path = config['database']['path']
db_name = config['database']['name']

# COMMAND ----------

_ = dbutils.fs.mkdirs(db_path)

# COMMAND ----------

import mlflow
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/kshare'.format(username))

# COMMAND ----------

_ = sql("""
CREATE DATABASE IF NOT EXISTS {db_name}
LOCATION '{db_path}/database'
""".format(db_name=db_name, db_path=db_path))

# COMMAND ----------

_ = sql("USE {db_name}".format(db_name=db_name))

# COMMAND ----------

def teardown():
  _ = sql("DROP DATABASE IF EXISTS {} CASCADE".format(db_name))
  dbutils.fs.rm(db_path, True)
