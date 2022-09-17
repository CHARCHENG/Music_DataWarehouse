package dhu.Charlie.utills

import org.apache.spark.sql.SparkSession

object LoadingDataFromESWithSparkSql {
  def loadingDataFromEs(session: SparkSession, indexName: String, tableName: String): Unit = {
    val sessionDataFrame = session.sqlContext
      .read.format("org.elasticsearch.spark.sql")
      .option("inferSchema", "true")
      .option("es.field.read.empty.as.null", "no")
      .load(indexName)

    sessionDataFrame.createTempView(tableName)
  }
}
