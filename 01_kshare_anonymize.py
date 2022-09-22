# Databricks notebook source
# MAGIC %md
# MAGIC ## Anonymizing transaction data
# MAGIC 
# MAGIC In this notebook, we will make use of [geohash](https://en.wikipedia.org/wiki/Geohash) to embed latitudes and longitudes into a hierarchical grid system that can be used for anonymization. Spanning from a few mm square (precision 12) to a thousand of kilometers (precision 1), Geohash will help us aggregate transactions bottom up through this hierarchy until we fully preserve our [k-anonymity](https://en.wikipedia.org/wiki/K-anonymity) criteria. We will leverage the semantic property of a Geohash Base32 representation where a parent geoHash always shares the same prefix as a children hash (polygon `DR5REGJ0ZYSY` is fully included in `DR5REGJ0ZYS`, all part of the `DR5`, `DR`, `D` branch of our tree structure)
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC <img src="https://chrysohous.files.wordpress.com/2012/09/300px-maidenhead_grid_over_europe.png" width=300>

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Acquiring geolocated card transaction data
# MAGIC Since we do not have public card transaction dataset available, we will be using the [NYC taxi dataset](https://data.cityofnewyork.us/Transportation/2014-Yellow-Taxi-Trip-Data/gkne-dk5s) as an example of geolocated card transaction data (using the tip amount relative to a trip distance). Before persisting our raw transactions as-is, we convert latitude and longitude into a GeoHash object using a simple `UDF`. As our network of geohashes can become fairly big at its highest granularity given a large dataset, we can control the maximum granularity (maximum 12) to pre-aggregate data and limit the network operations as defined later.

# COMMAND ----------

from pyspark.sql import functions as F 
from pyspark.sql.functions import udf 
import geohash
import urllib.request

# COMMAND ----------

urllib.request.urlretrieve('https://s3-us-west-2.amazonaws.com/nyctlc/nyc_taxi_data.csv.gz','/dbfs/{}/nyc_taxi_data.csv.gz'.format(db_path))

# COMMAND ----------

raw_transactions = (
  spark
    .read
    .format('csv')
    .option('header', 'true')
    .load('{}/nyc_taxi_data.csv.gz'.format(db_path))
    .select(
      F.to_timestamp('dropoff_datetime').alias('timestamp'),
      F.col('dropoff_latitude').cast('DOUBLE').alias('latitude'),
      F.col('dropoff_longitude').cast('DOUBLE').alias('longitude'),
      (F.col('tip_amount').cast('DOUBLE') / F.col('trip_distance').cast('DOUBLE')).alias('amount')
    )
    .filter(F.col('amount').isNotNull())
    .repartition(int(config['environments']['executors']))
    .cache()
)

raw_transactions.count()

# COMMAND ----------

@udf('string')
def geohash_udf(lat, lon):
  try:
    maxPrecision = 8
    return geohash.encode(lat, lon, maxPrecision)
  except:
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC As our data may be quite nosiy, we will only focus on a specific area of NYC described by geohash `dr5ru`. See [website](https://www.movable-type.co.uk/scripts/geohash.html) for more visual information of GeoHashing. 
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/kanonymity/geohash_nyc.png" width=500>

# COMMAND ----------

_ = (
  raw_transactions
    .withColumn('geohash', geohash_udf(F.col('latitude'), F.col('longitude'))) 
    .filter(F.col('geohash').isNotNull())
    .filter(F.col('geohash').like('dr5ru%'))
    .write
    .format('delta')
    .saveAsTable('transactions')
)

# COMMAND ----------

transactions = spark.read.table('transactions')
display(transactions.limit(10))

# COMMAND ----------

transactions.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Despite our restrictive filter, we still have 78,222,317 transactions we would like to anonymize with a maximum granularity allowed of size 8 (~ 30x30 meters). 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a hierarchical geohash structure
# MAGIC 
# MAGIC We first start by creating an hierarchical structure that can be used to aggregate transactions records and ensure our k-anonymity. Addressing this problem as a graph, each geohash must be connected to its closest parent, starting from granularity 12 (or `maxPrecision` defined earlier) all the way up to granularity 1. To build these edges, we leverage the semantic property of a geohash Base32 string representation, applying a sliding window of 2 over the 12 characters of a geohash string representation.
# MAGIC 
# MAGIC ```
# MAGIC DR5REGJ0ZYSY -> DR5REGJ0ZYS
# MAGIC DR5REGJ0ZYS  -> DR5REGJ0ZY
# MAGIC DR5REGJ0ZY   -> DR5REGJ0Z
# MAGIC DR5REGJ0Z    -> DR5REGJ0
# MAGIC DR5REGJ0     -> DR5REGJ
# MAGIC DR5REG       -> DR5REG
# MAGIC DR5RE        -> DR5R
# MAGIC DR5R         -> DR5
# MAGIC DR5          -> DR
# MAGIC DR           -> D
# MAGIC ```
# MAGIC 
# MAGIC We can render this simple tree as a graph using the [GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html) API. Note that we will be operating at a really low level here, hence requiring some serious coding (all commented) that may not be compatible with some high level APIs such as [graphframes](https://graphframes.github.io/graphframes/docs/_site/index.html). This also is the reason why we chose Scala over Python. Our logical tree structure (a node is only connected to 1 parent) will be addressed as a directed, unweighted graph and can be represented as follows
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/kanonymity/kanonymity_1.png" width=500>
# MAGIC 
# MAGIC We also need to define our `Vertex` attributes as a case class object. We will be storing the geohash itself (its base32 representation) as a label, the number of transactions observed at the highest granularity as well as a boolean that will be used to detect K-anonymity breaches (a breach is observed when a given hash did not observe enough transaction record)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.rdd.RDD
# MAGIC import org.apache.spark.graphx._
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.graphx.PartitionStrategy

# COMMAND ----------

# MAGIC %scala
# MAGIC val maxPrecision = spark.sql("SELECT MAX(LENGTH(geohash)) AS precision FROM transactions").collect.map(_.getAs[Int]("precision")).head
# MAGIC val k = 500 // Our anonymity threshold

# COMMAND ----------

# MAGIC %scala
# MAGIC case class Vertex(
# MAGIC   label: String,          // The geohash base32 representation
# MAGIC   observations: Long,     // The number of observations at that specific precision
# MAGIC   breach: Boolean = true  // Whether we observed enough transactions
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC We create our initial graph by reading all geohash and creating `child <> parent` tuple using a sliding window of 2. 

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def buildHierarchy(df: DataFrame, geohashCol: String): Graph[Vertex, Long] = {
# MAGIC   
# MAGIC   // We apply a simple sliding window on a geohash base32 representation
# MAGIC   // Each child will be connected to its parent
# MAGIC   // A child only has 1 parent
# MAGIC   val hashTuples = udf((hash: String) => {
# MAGIC     val hiearchy = (1 to maxPrecision).map(i => hash.substring(0, i)).reverse
# MAGIC     hiearchy.sliding(2).map(t => (t.head, t.last)).toSeq
# MAGIC   })
# MAGIC 
# MAGIC   val tuples = df
# MAGIC     .select(explode(hashTuples(col(geohashCol))).alias("tuple"))
# MAGIC     .select(col("tuple._1").alias("src"), col("tuple._2").alias("dst"))
# MAGIC     .distinct()
# MAGIC 
# MAGIC   val srcs = tuples.select(col("src").alias("label"))
# MAGIC   val dsts = tuples.select(col("dst").alias("label"))
# MAGIC 
# MAGIC   // We generate a unique identifier for each node in our graph
# MAGIC   val nodeIds = srcs.union(dsts).distinct()
# MAGIC     .orderBy(length(col("label")), col("label"))
# MAGIC     .rdd.map(_.getAs[String]("label"))
# MAGIC     .zipWithIndex().toDF("label", "id")
# MAGIC 
# MAGIC   // Even though we may start at highest granularity (a few mm square), we may still observe multiple transactions
# MAGIC   // We count the number of transactions as part of our vertex attribute
# MAGIC   val observations = df.groupBy(col(geohashCol).alias("label")).count()
# MAGIC 
# MAGIC   // We build vertices, joining with our observations (default is 0 observations)
# MAGIC   val nodes = nodeIds
# MAGIC     .join(observations, Seq("label"), "left_outer")
# MAGIC     .withColumn("count", when(col("count").isNull, lit(0)).otherwise(col("count")))
# MAGIC     .rdd.map(r => (r.getAs[Long]("id"), Vertex(r.getAs[String]("label"), r.getAs[Long]("count"))))
# MAGIC 
# MAGIC   // We build our directed, unweighted edges
# MAGIC   val edges = tuples
# MAGIC     .withColumnRenamed("src", "label").join(nodeIds, Seq("label")).withColumnRenamed("id", "src").drop("label")
# MAGIC     .withColumnRenamed("dst", "label").join(nodeIds, Seq("label")).withColumnRenamed("id", "dst").drop("label")
# MAGIC     .select("src", "dst").distinct()
# MAGIC     .rdd.map(r => Edge(r.getAs[Long]("src"), r.getAs[Long]("dst"), 0L))
# MAGIC   
# MAGIC   // We build our initial geohashing structure as a graphX object
# MAGIC   Graph.apply(nodes, edges)
# MAGIC   
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC We materialize our graph structure that we properly partition by collocating edges data sharing a same destination node. This partitioning may yield significant improvement when passing informations from parent to children, as covered later in our K-anonymity strategy.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Our graph is as a tree where every node only has 1 connection to its parent
# MAGIC // EdgePartition1D collocates edges having same source, we collocate edges having same destination
# MAGIC case object TreePartitioner extends PartitionStrategy {
# MAGIC   override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
# MAGIC     val mixingPrime: VertexId = 1125899906842597L
# MAGIC     (math.abs(dst) * mixingPrime).toInt % numParts
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC val df = spark.read.table("transactions")
# MAGIC val initGraph = buildHierarchy(df, "geohash").partitionBy(TreePartitioner)
# MAGIC initGraph.cache()
# MAGIC val numNodes = initGraph.vertices.count()
# MAGIC val numEdges = initGraph.edges.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Propagate observations
# MAGIC 
# MAGIC In the previous step, we simply laid the foundations to solve our k-anonymity constraint. Since we have captured the number of transactions at `maxPrecision` (children nodes), we need to propagate these observations higher up in the hierarchy so that each parent knows how many transactions its respective descendants have. With our controlled depth of 12 (or `maxPrecision`), we would need to repeat that operation of "message passing" 12 times. Luckily, [Pregel API](https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api) was built for that exact purpose. We will be starting by each child node (our geohash of precision 12) that will send the number of transactions they observed (`sendMessage`) to their parent. Each parent will aggregate the number of transactions from their children (`mergeMessage`) and will update themselves (`pregelCore`). Below is a high level representation of that process.
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/kanonymity/kanonymity_2.png" width=500>
# MAGIC 
# MAGIC The complexity is 2-fold. Firstly, we need to ensure a node will send a message to its parent only once or we're at risk of double counting transactions (not ideal when trying to ensure k-anonymity). Secondly, we need a breaking condition where pregel propagation will stop (i.e. when no message is sent through the network). We could define a maximum steps of 12 since we know our graph topology, but it always is a good practice to define that logic upfront. We wouldn't want to break our anonymity clause because of a simple typo on our pregel. Following our pregel strategy, we can easily identify any nodes (regardless of their position in our tree) with `breach = (num_transactions < k)`. This is represented as per below example for `k=3`.
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/kanonymity/kanonymity_3.png" width=500>

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def propagateObservations(graph: Graph[Vertex, Long], k: Int): Graph[Vertex, Long] = {
# MAGIC 
# MAGIC   // Each node will send the number of observations to their parents
# MAGIC   // If a node has no observation, it hasn't received a message from its children yet
# MAGIC   // If a node already has observations, we do not send any message (already sent at previous steps)
# MAGIC   // This ensures each node will send a message only once to its parents
# MAGIC   val sendMessage = (et: EdgeTriplet[Vertex, Long]) => 
# MAGIC     if(et.srcAttr.observations > 0 && et.dstAttr.observations == 0) {
# MAGIC       Iterator((et.dstId, et.srcAttr.observations))
# MAGIC     } else {
# MAGIC       Iterator.empty
# MAGIC     }
# MAGIC   
# MAGIC   // When a parent receive messages from multiple children, we need to aggregate messages
# MAGIC   // Given that we want to observe the number of transactions, it becomes a simple sum
# MAGIC   val mergeMessage = (m1: Long, m2: Long) => m1 + m2
# MAGIC 
# MAGIC   // A parent updates itself with the total number of observations reported by its children
# MAGIC   val pregelCore = (_: VertexId, vData: Vertex, m: Long) => vData.copy(observations = vData.observations + m)
# MAGIC 
# MAGIC   // We initialize pregel with an empty message (0 observations) and a maximum number of steps (12)
# MAGIC   // Note that we could change this number to Integer.MAX_INT as Pregel will still stop given our breaking condition
# MAGIC   val propagatedGraph = graph.pregel[Long](0L, maxPrecision, EdgeDirection.Out)(pregelCore, sendMessage, mergeMessage)
# MAGIC   
# MAGIC   // We detect every node (regardless of its hoerarchical level) with possible k-anonymity breach
# MAGIC   propagatedGraph.mapVertices((_, vData) => vData.copy(breach = vData.observations < k))
# MAGIC 
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC val observationGraph = propagateObservations(initGraph, k)
# MAGIC val observationNodes = observationGraph.vertices.map({ i => 
# MAGIC   (i._1, i._2.label, i._2.observations, i._2.breach)
# MAGIC }).toDF("id", "geohash", "observations", "breach")
# MAGIC display(observationNodes.orderBy(desc("observations"), length(col("geohash"))))

# COMMAND ----------

# MAGIC %md
# MAGIC Our structure is now fully aware of the number of transactions observed across all its different branches. As represented below, we can successfully observe all of our 78 million transactions at our highest hierarchical level (lowest granularity). 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve K anonymous geohashes
# MAGIC 
# MAGIC Here comes the crux of the problem. We need to find the highest granularity possible that would preserve K-anonymity, allowing the system to dynamically explore deeper layers for denser areas with many more transactions. Let's define our golden rule: **For anonymity to be preserved in a node at granularity G, we need to ensure all its neighbours also preserve K-anonymity at granularity G**. If any "sibling" node (a geohash connected to a same parent) breaches our k-anonymity constraint, we have to downgrade the whole block to a lower precision (moving 1 level up in the hierarchical structure).
# MAGIC 
# MAGIC In our visual example, we observe one valid node on the lower left branch with 3 transactions (hence preserving k-anonymity of 3). However, other nodes within the same branch at a same granularity are breaching this condition (they both only have 1 transaction observed). In the first part of our algorithm, we ensure each node breaching anonymity reports back to their parent node. This is done using [message passing](https://spark.apache.org/docs/latest/graphx-programming-guide.html#aggregate-messages-aggregatemessages) API. Each parent will be aware than one of its children is not "safe", hence invalidating their entire descendance through another Pregel call. This message passing and secondary Pregel is represented below. 
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/kanonymity/kanonymity_4.png" width=450>
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/kanonymity/kanonymity_5.png" width=450>
# MAGIC 
# MAGIC In order to optimize this process and minimize the number of Pregel operations, we did not simply invalidate a whole family tree by marking nodes as "breach", but we did so by propagating the label of the closest valid ancestor. The benefit of that process is that each node - regardless of its hierarchical level - will know the geohash representation of their closest relative that preserves our k-anonymity criteria. This process is described below
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/kanonymity/kanonymity_7.png" width=500>

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def detectBreaches(graph: Graph[Vertex, Long]) = {
# MAGIC   
# MAGIC   // **************** MESSAGE PASSING ****************
# MAGIC   // Any child breaching anonymity reports to its parent
# MAGIC   // A parent with at least one of its children unsafe marks itself unsafe
# MAGIC   
# MAGIC   val informParent = (ec: EdgeContext[Vertex, Long, Boolean]) => {
# MAGIC     if (ec.srcAttr.breach) ec.sendToDst(true)
# MAGIC   }
# MAGIC   val parentKnowsNodes = graph.aggregateMessages[Boolean](informParent, _|_)
# MAGIC   val parentKnowsGraph = graph.outerJoinVertices(parentKnowsNodes)((_, vData, v) => {
# MAGIC     vData.copy(breach = v.getOrElse(false))
# MAGIC   })
# MAGIC   
# MAGIC   // **************** BACK PROPAGATE ****************
# MAGIC   // Any unsafe parent informs its entire descendance, sending its geohash label to its children
# MAGIC   // At the end of Pregel, each node knows the label of their closest ancestor that guarantees k-anonymity
# MAGIC   
# MAGIC   def shouldPropagate(et: EdgeTriplet[Vertex, Long]) = et.dstAttr.breach && et.dstAttr.label != et.srcAttr.label
# MAGIC   val sendMessage = (et: EdgeTriplet[Vertex, Long]) => if(shouldPropagate(et)) {
# MAGIC     Iterator((et.srcId, et.dstAttr.label))
# MAGIC   } else {
# MAGIC     Iterator.empty
# MAGIC   }
# MAGIC   
# MAGIC   val mergeMessage = (m1: String, m2: String) => m1 // a child only has 1 parent
# MAGIC   val pregelCore = (_: VertexId, vData: Vertex, label: String) => if(label.nonEmpty) {
# MAGIC     vData.copy(breach = true, label = label)
# MAGIC   } else {
# MAGIC     vData
# MAGIC   }
# MAGIC   parentKnowsGraph.pregel[String]("", maxPrecision, EdgeDirection.In)(pregelCore, sendMessage, mergeMessage)
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC We can create a simple mapping table of `geohash <> anonymized_geohash` that can be used against our original transactions. To do so, we extract nodes with no incoming edges (`inDegrees == 0`). These nodes are the original geohashes at highest granularity updated with the geohash labels of their closest k-anonymous ancestor. 

# COMMAND ----------

# MAGIC %scala
# MAGIC def getAnonymizedNodes(graph: Graph[Vertex, Long]) = {
# MAGIC   graph.outerJoinVertices(graph.inDegrees)((vId, vData, vDeg) => {
# MAGIC     (vData, vDeg.getOrElse(0))
# MAGIC   }).vertices.filter({ case (_, (_, vDeg)) => vDeg == 0}).map({ case (vId, (vData, vDeg)) =>
# MAGIC     (vId, vData.label)
# MAGIC   }).toDF("id", "anonymized_geohash")
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC Although our labels have changed between our initial graph and our new representations (geohashes are inherited from their closest safe parents), the vertex IDs remained the same and therefore can be joined to create our mapping table.

# COMMAND ----------

# MAGIC %scala
# MAGIC val anonymityGraph = detectBreaches(observationGraph)
# MAGIC val anonymityNodes = getAnonymizedNodes(anonymityGraph)
# MAGIC val anonymizedMap = anonymityNodes.join(observationNodes, List("id")).select("geohash", "anonymized_geohash")
# MAGIC anonymizedMap.write.format("delta").saveAsTable("transactions_mapping")
# MAGIC display(anonymizedMap)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anonymize transactions
# MAGIC Here we are, equipped with a mapping table that can be used against our original transactions. We simply join our records against geohash of lowest granularity, get their anonymized geohash equivalent and group our transactions to get anonymized statistics (such as the average spent for each location). We can easily store the results back to a gold table that would preserve the monetary value of our transaction data without impacting customers privacy

# COMMAND ----------

transactions_anonymized = (
  spark.read.table('transactions')
    .join(spark.read.table('transactions_mapping'), ['geohash'])
    .groupBy('anonymized_geohash')
    .agg(
      F.avg('amount').alias('average_transaction'),
      F.sum(F.lit(1)).alias('number_transactions')
    )
)

transactions_anonymized.write.format('delta').saveAsTable('transactions_anonymized')
display(transactions_anonymized)

# COMMAND ----------

# MAGIC %md
# MAGIC As reported above, with a k-anonymity of 500, we confirm that our anonymization structure did not return any location with less than 500 observed transactions and grouped our 78 millions of transactions into 7100 distinct clusters. Although some areas may contain transactions close to our anonymity threshold, some exhibits much more transactions (114,000 transactions for `dr5rukv`). The reason is that, despite dense areas, at least one of their children geohash was breaching our k-anonymity threshold, hence moving the whole block to a higher hierarchical level (lower granularity). In order to validate our approach further, we can render our aggregated transactions on a map using `folium`, `geopandas` and `polygon_geohasher` python libraries

# COMMAND ----------

import folium
import geopandas
from polygon_geohasher.polygon_geohasher import geohash_to_polygon
from pyspark.sql import functions as F

# COMMAND ----------

# retrieve anonymized data and create pandas dataframe
df = spark.read.table('transactions_anonymized').toPandas()

# convert pandas to geopandas dataframe with polygon definition of our geohash
gdf = geopandas.GeoDataFrame(df)
gdf['geometry'] = gdf['anonymized_geohash'].apply(geohash_to_polygon)
gdf.crs = {'init': 'epsg:4326'}

# visualize aggregated transactions on a map, color coded by average spent
nyc = folium.Map([40.76,-73.9857], zoom_start=14, width='80%', height='100%')
folium.Choropleth(
  geo_data=gdf, 
  name='choropleth',
  data=gdf,
  columns=['anonymized_geohash', 'average_transaction'],
  key_on='feature.properties.anonymized_geohash',
  fill_color="BuPu",
  fill_opacity=0.8,
  line_opacity=0.2,
  legend_name='asdf'
).add_to(nyc)

nyc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Not only we ensured no group had less that 500 transactions, but the system we created allowed for different granularities to be used for different region densities. In some places with more transactions, we could afford to go at much higher granularity yet to preserve our 500 transactions clause. In some other regions, at least one geohash would not satisfy our k-anonymity clause. Note that we explicitly did not allow more than granularity 8 in our geohash strategy here, but this framework can easily be extended to reach 1mm x 1mn granularity (it would be impossible to render on a map for visualization purpose).

# COMMAND ----------

display(transactions_anonymized.orderBy(F.desc('average_transaction')))

# COMMAND ----------

# MAGIC %md
# MAGIC One can notice a really small area around Time Square color coded in purple. This place is where we observed 2 times more tips in average for 770 customers. Getting back to our original NYC taxi dataset, it seems that dropping off tourists at the Intercontinental hotel in Time Square seems to generate much greater tips! We learned so using at least 500 trips, so **impossible to know who the best tipper was**.
# MAGIC 
# MAGIC ___
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/kanonymity/timesquare_tips.png" width=500>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving data via delta sharing
# MAGIC As we now have built a dataset that is fully anonymized, we can safely monetize its content to third party companies. However, although we demonstrated the usefulness of geohashing with regards to geospatial analytics, it may not be as easy for any company to comprehend, decipher and act upon. Instead, we convert our geohash values into simple polygon shapes that can be used by any GIS library such as `geopandas`. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish data to AWS S3 bucket
# MAGIC We start by writing our anonymized dataset to a separate table we surface to our main customer, `asset_mgmt_acme` company.

# COMMAND ----------

import geopandas as gpd
import pandas as pd
from polygon_geohasher.polygon_geohasher import geohash_to_polygon

# COMMAND ----------

# retrieve anonymized data and create pandas dataframe
df = spark.read.table('transactions_anonymized').toPandas()

# convert pandas to geopandas dataframe with polygon definition of our geohash
gdf = gpd.GeoDataFrame(df)
gdf['geometry'] = gdf['anonymized_geohash'].apply(geohash_to_polygon)
gdf['geometry']

# convert polygons to well known text (wkt)
def get_wkt(geom):
  return geom.wkt

gdf['polygon'] = gdf['geometry'].apply(get_wkt)
gdf['polygon_id'] = gdf.index

spark \
  .createDataFrame(gdf[['polygon_id', 'polygon', 'average_transaction', 'number_transactions']]) \
  .write \
  .format('delta') \
  .mode('overwrite') \
  .saveAsTable('transactions_anonymized_share')

# COMMAND ----------

display(spark.read.table('transactions_anonymized_share'))

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook, we demonstrated how Geopatial and Graph analytics are often intertwined and require access to different libraries and flexibile compute that often does not fit well with traditional data warehousing paradigm. We demonstrated how organizations can build a powerful data structure that will be used to anonymize card transactions data at a geographical level, allowing them to provide their customers much more granular insights for denser regions. Finally, we demonstrated how these insights could be safely shared to external customers using delta share capability. 
