/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.examples.performance

import java.io.File
import java.sql.{DriverManager, ResultSet}

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.datanucleus.store.rdbms.connectionpool.DatastoreDriverNotFoundException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hive.server.HiveEmbeddedServer2

case class Query(sqlText: String)

case class HiveOrcTablePerformance(tableName: String, query: String, time: String)

case class HiveCarbonTablePerformance(tableName: String, query: String, time: String)

object CompareTestHive {

  val hiveCarbonTableName = "comparetest_hive_carbon"
  val hiveOrcTableName = "hivetable"

  val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val warehouse = s"$rootPath/integration/hive/target/warehouse"
  val metastoredb = s"$rootPath/integration/hive/target/comparetest_metastore_db"

  val carbonTableName = "comparetest_hive_carbon"

  var resultSet: ResultSet = _

  def main(args: Array[String]) {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")

    import org.apache.spark.sql.CarbonSession._

    val carbon = SparkSession
      .builder()
      .master("local")
      .appName("CompareTestExample")
      .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
      .getOrCreateCarbonSession(
        "hdfs://localhost:54310/user/hive/warehouse", metastoredb)

    val hiveEmbeddedServer2 = new HiveEmbeddedServer2
    hiveEmbeddedServer2.start()
    val port = hiveEmbeddedServer2.getFreePort

    Try(Class.forName("org.apache.hive.jdbc.HiveDriver")).getOrElse(
      throw new DatastoreDriverNotFoundException("driver not found "))

    val con = DriverManager
      .getConnection(s"jdbc:hive2://localhost:$port/default", "anonymous", "anonymous")
    val stmt = con.createStatement

    println(s"============HIVE CLI IS STARTED ON PORT $port ==============")

    stmt.execute("DROP TABLE IF EXISTS ORDERS1")
    stmt.execute("DROP TABLE IF EXISTS customer1")
    stmt.execute("DROP TABLE IF EXISTS LINEITEM")
    stmt.execute("DROP TABLE IF EXISTS NATION")
    stmt.execute("DROP TABLE IF EXISTS REGION")
    stmt.execute("DROP TABLE IF EXISTS PART")
    stmt.execute("DROP TABLE IF EXISTS PARTSUPP")
    stmt.execute("DROP TABLE IF EXISTS SUPPLIER")

    carbon.sql("DROP TABLE IF EXISTS ORDERS1")
    carbon.sql("DROP TABLE IF EXISTS customer1")
    carbon.sql("DROP TABLE IF EXISTS LINEITEM")
    carbon.sql("DROP TABLE IF EXISTS NATION")
    carbon.sql("DROP TABLE IF EXISTS REGION")
    carbon.sql("DROP TABLE IF EXISTS REGION")
    carbon.sql("DROP TABLE IF EXISTS PART")
    carbon.sql("DROP TABLE IF EXISTS PARTSUPP")
    carbon.sql("DROP TABLE IF EXISTS SUPPLIER")

    println("------------------For LINE ITEM -----------------------------")

    carbon.sql("CREATE TABLE LINEITEM( L_ORDERKEY INT ,L_PARTKEY INT ,L_SUPPKEY INT ," +
               "L_LINENUMBER INT ,L_QUANTITY DECIMAL(15,2)," +
               "L_EXTENDEDPRICE DECIMAL(15,2) ,L_DISCOUNT DECIMAL" +
               "(15,2),L_TAX DECIMAL(15,2) ,L_RETURNFLAG STRING," +
               " L_LINESTATUS STRING ,L_SHIPDATE TIMESTAMP ,L_COMMITDATE TIMESTAMP" +
               " ,L_RECEIPTDATE TIMESTAMP ,L_SHIPINSTRUCT STRING" +
               " ,L_SHIPMODE STRING ,L_COMMENT STRING ) STORED BY 'CARBONDATA' ")

    carbon.sql("LOAD DATA INPATH 'hdfs://localhost:54310/user/lineitem.csv' INTO TABLE lineitem " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' L_ORDERKEY,L_PARTKEY," +
               "L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG," +
               "L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE," +
               "L_COMMENT')")


    stmt.execute(" CREATE TABLE LINEITEM " +
                 "(L_ORDERKEY INT ,L_PARTKEY INT ,L_SUPPKEY INT , L_LINENUMBER INT ," +
                 "L_QUANTITY DECIMAL(15,2) ,L_EXTENDEDPRICE DECIMAL(15,2) ,L_DISCOUNT DECIMAL(15,2) ," +
                 "L_TAX DECIMAL(15,2) ,L_RETURNFLAG STRING , L_LINESTATUS STRING ,L_SHIPDATE TIMESTAMP ," +
                 "L_COMMITDATE TIMESTAMP ,L_RECEIPTDATE TIMESTAMP ,L_SHIPINSTRUCT STRING ,L_SHIPMODE STRING ," +
                 "L_COMMENT STRING )")

    stmt
      .execute(
        "ALTER TABLE LINEITEM SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt.execute("ALTER TABLE LINEITEM SET LOCATION 'hdfs://localhost:54310/user/hive/warehouse/default/lineitem'")

    println("------------------For NATION -----------------------------")

    carbon.sql("CREATE TABLE NATION ( N_NATIONKEY INT,\n N_NAME STRING,\n N_REGIONKEY INT,\n " +
               "N_COMMENT STRING) STORED BY 'carbondata' ")

    carbon.sql("LOAD DATA INPATH 'hdfs://localhost:54310/user/nation.csv' INTO TABLE nation " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='N_NATIONKEY,N_NAME," +
               "N_REGIONKEY,N_COMMENT')")

    stmt.execute("CREATE TABLE NATION (N_NATIONKEY INT,\n N_NAME STRING,\n N_REGIONKEY INT,\n " +
                 "N_COMMENT STRING)")
    stmt
      .execute(
        "ALTER TABLE NATION SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")
    stmt
      .execute(
        "ALTER TABLE NATION SET LOCATION " +
        s"'hdfs://localhost:54310/hdfs://localhost:54310/user/hive/warehouse/default/nation' ".stripMargin)

    println("------------------For customer1 -----------------------------")


    carbon.sql("CREATE TABLE customer1 ( C_CUSTKEY INT ,\n C_NAME STRING ,\n C_ADDRESS STRING ,\n " +
               "C_NATIONKEY INT ,\n C_PHONE STRING ,\n C_ACCTBAL DECIMAL(15,2) ,\n C_MKTSEGMENT " +
               "STRING ,\n C_COMMENT STRING ) STORED BY 'carbondata' ")

    carbon
      .sql("LOAD DATA INPATH \"hdfs://localhost:54310/user/customer.csv\" INTO TABLE customer1 " +
           "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"' , 'FILEHEADER'='C_CUSTKEY,C_NAME," +
           "C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')")

    stmt.execute("CREATE TABLE customer1 ( C_CUSTKEY INT ,\n C_NAME STRING ,\n C_ADDRESS STRING ,\n " +
               "C_NATIONKEY INT ,\n C_PHONE STRING ,\n C_ACCTBAL DECIMAL(15,2) ,\n C_MKTSEGMENT " +
               "STRING ,\n C_COMMENT STRING )")

    stmt
      .execute(
        "ALTER TABLE customer1 SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt.execute("ALTER TABLE customer1 SET LOCATION 'hdfs://localhost:54310/user/hive/warehouse/default/customer1'".stripMargin)

    println("------------------For ORDERS -----------------------------")

    carbon
      .sql(
        "CREATE TABLE ORDERS1 ( O_ORDERKEY INT ,O_CUSTKEY INT ,O_ORDERSTATUS STRING ,O_TOTALPRICE " +
        "DECIMAL(15,2) , O_ORDERDATE TIMESTAMP , O_ORDERPRIORITY STRING , O_CLERK STRING , " +
        "O_SHIPPRIORITY INT , O_COMMENT STRING ) STORED BY 'carbondata' ")

    carbon.sql("LOAD DATA INPATH 'hdfs://localhost:54310/user/orders.csv' INTO TABLE orders1 " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='O_ORDERKEY,O_CUSTKEY," +
               "O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY," +
               "O_COMMENT')")

    stmt.execute(
        "CREATE TABLE ORDERS1 ( O_ORDERKEY INT ,O_CUSTKEY INT ,O_ORDERSTATUS STRING ,O_TOTALPRICE " +
        "DECIMAL(15,2) , O_ORDERDATE TIMESTAMP , O_ORDERPRIORITY STRING , O_CLERK STRING , " +
        "O_SHIPPRIORITY INT , O_COMMENT STRING )")

    stmt
      .execute(
        "ALTER TABLE ORDERS1 SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt.execute("ALTER TABLE ORDERS1 SET LOCATION 'hdfs://localhost:54310/user/hive/warehouse/default/orders1'")


    println("------------------QUERY EXECUTION-----------------------------")

   stmt.execute("set hive.mapred.supports.subdirectories=true")
   stmt.execute("set mapreduce.input.fileinputformat.input.dir.recursive=true")
    getQueries.zipWithIndex.foreach {
      case (query, index) =>
        time {
          println("************************EXECUTIED QUERY ***********************" + query.sqlText)
          val res: ResultSet =stmt.executeQuery(query.sqlText)
          while (res.next()){
            println("&*************************************************************88")
          }
        }
    }

    System.exit(0)

  }


  private def getQueries: Array[Query] = {
    Array(
      // ===========================================================================
      // ==                     FULL SCAN AGGREGATION                             ==
      // ===========================================================================
      //failed

      Query("select * from orders1 limit 10"),
      Query("select l_orderkey from lineitem"),
      Query("select\n        l_orderkey,\n        sum(l_extendedprice * (1 - l_discount)) as " +
            "revenue,\n        o_orderdate,\n        o_shippriority\nfrom\n        customer1,\n   " +
            "     orders1,\n        lineitem\nwhere\n        c_mktsegment = 'BUILDING'\n        " +
            "and c_custkey = o_custkey\n        and l_orderkey = o_orderkey\n        and " +
            "o_orderdate < '1995-03-22'\n        and l_shipdate > '1995-03-22'\ngroup by\n       " +
            " l_orderkey,\n        o_orderdate,\n        o_shippriority\norder by\n        " +
            "revenue desc,\n        o_orderdate\nlimit 10")
    )
  }

  // Run all queries for the specified table
  private def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }

}


