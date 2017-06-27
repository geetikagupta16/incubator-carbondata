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

package org.apache.carbondata.presto

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}
import java.util
import java.util.{Calendar, Locale, Optional}

import com.facebook.presto.Session
import com.facebook.presto.execution.QueryIdGenerator
import com.facebook.presto.metadata.SessionPropertyManager
import com.facebook.presto.spi.`type`.TimeZoneKey.UTC_KEY
import com.facebook.presto.spi.security.Identity
import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap
import org.slf4j.{Logger, LoggerFactory}

object PrestoBenchMarking {
  val rootPath: String = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  val CARBONDATA_CATALOG = "carbondata"
  val CARBONDATA_CONNECTOR = "carbondata"
  val CARBONDATA_STOREPATH = s"$rootPath/integration/presto/test/store"
  val CARBONDATA_SOURCE = "carbondata"

  // Instantiates the Presto Server to connect with the Apache CarbonData
  @throws[Exception]
  def createQueryRunner(extraProperties: util.Map[String, String]): DistributedQueryRunner = {
    val queryRunner = new DistributedQueryRunner(createSession, 4, extraProperties)
    try {
      queryRunner.installPlugin(new CarbondataPlugin)
      val carbonProperties = ImmutableMap.builder[String, String]
        .put("carbondata-store", CARBONDATA_STOREPATH).build

      // CreateCatalog will create a catalog for CarbonData in etc/catalog.
      queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties)
      queryRunner
    } catch {
      case e: Exception =>
        queryRunner.close()
        throw e
    }
  }

  // CreateSession will create a new session in the Server to connect and execute queries.
  def createSession: Session = {
    Session.builder(new SessionPropertyManager)
      .setQueryId(new QueryIdGenerator().createNextQueryId)
      .setIdentity(new Identity("user", Optional.empty()))
      .setSource(CARBONDATA_SOURCE).setCatalog(CARBONDATA_CATALOG)
      .setTimeZoneKey(UTC_KEY).setLocale(Locale.ENGLISH)
      .setRemoteUserAddress("address")
      .setUserAgent("agent").build
  }

  // Creates a JDBC Client to connect CarbonData to Presto
  @throws[Exception]
  def prestoJdbcClient(): Option[Array[Double]] = {
    val logger: Logger = LoggerFactory.getLogger("Presto Server on CarbonData")
    logger.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = createQueryRunner(
      ImmutableMap.of("http-server.http.port", "8086"))
    Thread.sleep(10)
    logger.info("STARTED SERVER AT :" + queryRunner.getCoordinator.getBaseUrl)

    // Step 1: Create Connection Strings
    val JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver"
    val DB_URL = "jdbc:presto://localhost:8086/carbondata/default"

    // The database Credentials
    val USER = "username"
    val PASS = "password"
    try {
      logger.info("=============Connecting to database/table" +
                  "===============")
      // STEP 2: Register JDBC driver
      Class.forName(JDBC_DRIVER)
      // STEP 3: Open a connection
      val conn: Connection = DriverManager.getConnection(DB_URL, USER, PASS)
      val stmt: Statement = conn.createStatement
      val tableName = "comparetest_carbonV3"

      val executionTime: Array[Double] = BenchMarkingUtil.queries.map { queries =>
        // val query = queries.sqlText.replace("$table", tableName)
        val time = BenchMarkingUtil.time(stmt.executeQuery(queries.sqlText))
        println("\nSuccessfully Executed the Query : " + queries.sqlText + "\n")
        time
      /*  val res: ResultSet =stmt.executeQuery(queries.sqlText)
        res.last()
        println("\nSuccessfully Executed the Query : " + queries.sqlText + "\n and result size is :" + res.getRow() + "\n")
        0.01*/
      }
      conn.close()
      Some(executionTime)
    } catch {
      case se: SQLException =>
        // Handle errors for JDBC
        logger.error(se.getMessage)
        None
      case e: Exception =>
        // Handle errors for Class.forName
        logger.error(e.getMessage)
        None
    } finally {
      queryRunner.close()
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val rootFilePath = s"$rootPath/integration/presto/data/"
    val compareFile = s"$rootFilePath/BenchmarkingResults.txt"
    val prestoFile = s"$rootFilePath/PrestoBenchmarkingResults.txt"
    val parquetFile = s"$rootFilePath/ParquetBenchmarkingResults.txt"
  //  val orcFile = s"$rootFilePath/OrcBenchmarkingResults.txt"
    val carbonFile= s"$rootFilePath/CarbonDataBenchmarkingResults.txt"
    val util: BenchMarkingUtil.type = BenchMarkingUtil
    // Garbage Collection
    System.gc()

    prestoJdbcClient().foreach { (x: Array[Double]) =>
      x foreach { (z: Double) =>
        util.writeResults("" + z + "\n", prestoFile)
      }
    }

     val parquetResults: List[String] = util.readFromFile(parquetFile)
     val prestoResults: List[String] = util.readFromFile(prestoFile)
     val carbonResults: List[String] = util.readFromFile(carbonFile)
     val aggResults: List[(String, String, String)] =
       (parquetResults, carbonResults, prestoResults).zipped.toList

     util.writeResults("\n\n-------------------------DATE/TIME:"
       + Calendar.getInstance().getTime() +
       " -----------------------------\n\n" ,compareFile)

     (BenchMarkingUtil.queries, aggResults).zipped.foreach { (query, results) =>
       val (parquetTime, carbonDataTime, prestoTime) = results

       val content= Tabulator.format(List(List("PARQUET", "CARBONDATA","PRESTO"), List(parquetTime, carbonDataTime,prestoTime)))
       util.writeResults("\n"+query.sqlText,compareFile)
       util.writeResults("\n"+content, compareFile)
     }
     // scalastyle:off
     util.readFromFile(compareFile).foreach(line => println(line))
     // scalastyle:on
    System.exit(0)
  }
}