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
package org.apache.carbondata.presto.server

import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.{Locale, Optional}

import com.facebook.presto.Session
import com.facebook.presto.execution.QueryIdGenerator
import com.facebook.presto.metadata.SessionPropertyManager
import com.facebook.presto.spi.`type`.TimeZoneKey.UTC_KEY
import com.facebook.presto.spi.security.Identity
import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap
import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.presto.CarbondataPlugin

object PrestoServer {

  val CARBONDATA_CATALOG = "carbondata"
  val CARBONDATA_CONNECTOR = "carbondata"
  val CARBONDATA_SOURCE = "carbondata"

  def startServer(carbonStorePath: String) = {
    val logger: Logger = LoggerFactory.getLogger("Presto Server on CarbonData")

    import scala.collection.JavaConverters._

    val prestoProperties: util.Map[String, String] = Map(("http-server.http.port", "8086")).asJava

    logger.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = createQueryRunner(
      prestoProperties, carbonStorePath)

    logger.info("STARTED SERVER AT :" + queryRunner.getCoordinator.getBaseUrl)
  }

  // Instantiates the Presto Server to connect with the Apache CarbonData
  private def createQueryRunner(extraProperties: util.Map[String, String],
      carbonStorePath: String): DistributedQueryRunner = {
    val queryRunner = new DistributedQueryRunner(createSession, 4, extraProperties)
    try {
      queryRunner.installPlugin(new CarbondataPlugin)
      val carbonProperties = ImmutableMap.builder[String, String]
        // .put("com.facebook.presto", "DEBUG")
        .put("carbondata-store", carbonStorePath).build

      // CreateCatalog will create a catalog for CarbonData in etc/catalog.
      queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties)
      queryRunner
    } catch {
      case exception: Exception =>
        queryRunner.close()
        throw exception
    }
  }

  // CreateSession will create a new session in the Server to connect and execute queries.
  private def createSession: Session = {
    Session.builder(new SessionPropertyManager)
      .setQueryId(new QueryIdGenerator().createNextQueryId)
      .setIdentity(new Identity("user", Optional.empty()))
      .setSource(CARBONDATA_SOURCE).setCatalog(CARBONDATA_CATALOG)
      .setTimeZoneKey(UTC_KEY).setLocale(Locale.ENGLISH)
      .setRemoteUserAddress("address")
      .setUserAgent("agent").build
  }

  def executeQuery(query: String): List[Map[String, Object]] = {

    // Creates a JDBC Client to connect CarbonData to Presto
    val JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver"
    val DB_URL = "jdbc:presto://localhost:8086/carbondata/testdb"

    // The database Credentials
    val USER = "username"
    val PASS = "password"

    // STEP 2: Register JDBC driver
    Class.forName(JDBC_DRIVER)
    // STEP 3: Open a connection
    val conn: Connection = DriverManager.getConnection(DB_URL, USER, PASS)
    val statement = conn.createStatement()
    val result: ResultSet = statement.executeQuery(query)
    resultSetToList(result)
  }

  def resultSetToList(queryResult: ResultSet): List[Map[String, Object]] = {
    val metadata = queryResult.getMetaData
    val colNames = (1 to metadata.getColumnCount) map metadata.getColumnName
    Iterator.continually(buildMap(queryResult, colNames)).takeWhile(_.isDefined).map(_.get).toList
  }

  private def buildMap(queryResult: ResultSet,
      colNames: Seq[String]): Option[Map[String, Object]] = {
    if (queryResult.next()) {
      Some(colNames.map(name => name -> queryResult.getObject(name)).toMap)
    }
    else {
      None
    }
  }

}
