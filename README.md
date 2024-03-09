<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-GCP-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)
[![POC](https://img.shields.io/badge/POC-5_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

*In 1973, world-famous American sculptor and video artist Richard Serra stated: "if something is free, you’re the product". No-one could have ever imagined how appropriate that statement could be fast forward 50 years. With a financial ecosystem made of free fintech apps, robo adviser, retail investment platforms and open banking aggregators, the commercial value for consumer data is huge. The most successful startups have soon realised the monetary value lying with consumer data and have shifted their business models from license based model to data centricity where products are often offered free of charge to their end consumers in exchange for their data. But here lies the conundrum: how can a company monetize data in a way that would preserve its value without impacting customer privacy? How could companies maintain the single biggest asset they have, the trust from their customers? Through this series of notebooks and reusable libraries around geospatial and graph analytics, we will demonstrate how lakehouse can be used to enforce k-anonymity of transactions data that can be monetized with high governance standards via delta sharing capabilities*

---
<antoine.amend@databricks.com>

___


IMAGE TO REFERENCE ARCHITECTURE

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                           | description               | license    | source                                              |
|-----------------------------------|---------------------------|------------|-----------------------------------------------------|
| folium (optional)                 | Geospatial visualization  | MIT        | https://github.com/python-visualization/folium      |
| geopandas (optional)              | Geospatial framework      | BSD        | https://geopandas.org/                              |  
| polygon_geohasher (optional)      | Geospatial framework      | MIT        | https://github.com/Bonsanto/polygon-geohasher       |  
| python-geohash                    | Geohashing framework      | MIT        | https://code.google.com/archive/p/python-geohash/   |  

To run this accelerator, clone this repo into a Databricks workspace. Attach the RUNME notebook to any cluster running a DBR 11.0 or later runtime, and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. Execute the multi-step-job to see how the pipeline runs.

The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.
