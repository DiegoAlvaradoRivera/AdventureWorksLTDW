package org.advworkslh.services

import org.apache.spark.sql.SparkSession

object SparkCluster {

  lazy val getSparkSession: SparkSession = {

    // if running on a dbs cluster, get the use getOrCreate()
    if (sys.env.contains("DATABRICKS_RUNTIME_VERSION")) {

      // Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

      SparkSession.builder().getOrCreate()

    }
    // if running on a local env, create the spark session
    else {

      val spark = SparkSession.builder().
        appName("AdventureWorksLTDW").
        master("local[8]").

        // set the warehouse directory
        config("spark.sql.warehouse.dir", "D:/tmp/spark-warehouse").

        // add the Delta Lake ...
        config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").
        config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").

        // set the OLTP DB connection string
        config("AdvWorksOLTPConnString", s"jdbc:sqlserver://localhost;databaseName=AdventureWorksOLTP;user=dbadmin;password=12345;").

        // set the OLTP DB connection string
        config("AdvWorksOLAPConnString", s"jdbc:sqlserver://localhost;databaseName=AdventureWorksOLAP;user=dbadmin;password=12345;").

        getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      spark.conf.set("spark.sql.shuffle.partitions", 24)

      spark
    }

  }

}
