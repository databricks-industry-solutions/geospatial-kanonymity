# Databricks notebook source
# MAGIC %md
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">
# MAGIC 
# MAGIC [![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
# MAGIC [![CLOUD](https://img.shields.io/badge/CLOUD-GCP-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC [![POC](https://img.shields.io/badge/POC-5 days-green?style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC 
# MAGIC *In 1973, world-famous American sculptor and video artist Richard Serra stated: "if something is free, you’re the product". No-one could have ever imagined how appropriate that statement could be fast forward 50 years. With a financial ecosystem made of free fintech apps, robo adviser, retail investment platforms and open banking aggregators, the commercial value for consumer data is huge. The most successful startups have soon realised the monetary value lying with consumer data and have shifted their business models from license based model to data centricity where products are often offered free of charge to their end consumers in exchange for their data. But here lies the conundrum: how can a company monetize data in a way that would preserve its value without impacting customer privacy? How could companies maintain the single biggest asset they have, the trust from their customers? Through this series of notebooks and reusable libraries around geospatial and graph analytics, we will demonstrate how lakehouse can be used to enforce k-anonymity of transactions data that can be monetized with high governance standards via delta sharing capabilities*
# MAGIC 
# MAGIC ---
# MAGIC <antoine.amend@databricks.com>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                           | description               | license    | source                                              |
# MAGIC |-----------------------------------|---------------------------|------------|-----------------------------------------------------|
# MAGIC | folium (optional)                 | Geospatial visualization  | MIT        | https://github.com/python-visualization/folium      |
# MAGIC | geopandas (optional)              | Geospatial framework      | BSD        | https://geopandas.org/                              |  
# MAGIC | polygon_geohasher (optional)      | Geospatial framework      | MIT        | https://github.com/Bonsanto/polygon-geohasher       |  
# MAGIC | python-geohash                    | Geohashing framework      | MIT        | https://code.google.com/archive/p/python-geohash/   |  
