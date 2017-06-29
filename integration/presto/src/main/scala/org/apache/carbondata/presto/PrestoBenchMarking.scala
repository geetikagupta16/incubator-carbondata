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

  // Creates a JDBC Client to connect CarbonData to Presto
  @throws[Exception]
  def prestoJdbcClient(): Option[Array[Double]] = {
    val logger: Logger = LoggerFactory.getLogger("Presto Server on CarbonData")

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

      val executionTime: Array[Double] = BenchMarkingUtil.queries.map { queries =>
        val time = BenchMarkingUtil.time(stmt.executeQuery(queries.sqlText))
        println("\nSuccessfully Executed the Query : " + queries.sqlText + "\n")
        time
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
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val rootFilePath = s"$rootPath/integration/presto/data/"
    val prestoFile = s"$rootFilePath/PrestoBenchmarkingResults"
    val util: BenchMarkingUtil.type = BenchMarkingUtil
    // Garbage Collection
    System.gc()

    prestoJdbcClient().foreach { (x: Array[Double]) =>
      val y: List[Double] = (x map { (z: Double) => z }).toList
      (BenchMarkingUtil.queries,y).zipped foreach{(query,time)=>
        util.writeResults(" [ Query :" +query + "\n"
          +"Time :"+ time + " ] \n\n "
          , prestoFile)
      }
    }
     // scalastyle:off
     util.readFromFile(prestoFile).foreach(line => println(line))
     // scalastyle:on
    System.exit(0)
  }
}
