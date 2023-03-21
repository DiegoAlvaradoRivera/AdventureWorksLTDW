// Databricks notebook source
import java.sql.{DriverManager, Driver, Connection, ResultSet}

// COMMAND ----------

// https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html

val drivers = DriverManager.getDrivers

while (drivers.hasMoreElements){
  val driver: Driver  = drivers.nextElement()
  println(s"$driver version ${driver.getMajorVersion}.${driver.getMinorVersion}")
}


// COMMAND ----------

val url = dbutils.secrets.get(scope="advworkslt", key="AdvWorksOLTPConnString")

val conn = DriverManager.getConnection(url)
val stmt = conn.createStatement()
val rs = stmt.executeQuery("SELECT DB_NAME() as dbname")

// COMMAND ----------

while(rs.next()){
  println("dbname: " + rs.getString("dbname"))
}

// COMMAND ----------

val url = dbutils.secrets.get(scope="advworkslt", key="AdvWorksOLTPConnString")

spark.read.
  format("jdbc").
  option("url", url).
  option("query", "SELECT DB_NAME() as dbname").
  load(). 
  show(truncate=false)

// COMMAND ----------

spark.range(1, 10).withColumnRenamed("id", "n").
  write.
  mode("append").
  format("jdbc").
  option("url", url).
  option("dbtable", "dbo.numbers").
  save()

// COMMAND ----------

spark.read.
  format("jdbc").
  option("url", url).
  option("dbtable", "dbo.numbers").
  load(). 
  show(truncate=false)

// COMMAND ----------


