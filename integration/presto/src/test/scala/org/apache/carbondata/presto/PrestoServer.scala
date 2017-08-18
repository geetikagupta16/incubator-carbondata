package org.apache.carbondata.presto

import java.io.File
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

class PrestoServer {
  val rootPath: String = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath

  val CARBONDATA_CATALOG = "carbondata"
  val CARBONDATA_CONNECTOR = "carbondata"
  val CARBONDATA_STOREPATH = s"$rootPath/integration/presto/test/store"
  val CARBONDATA_SOURCE = "carbondata"

  @throws[Exception]
  def prestoJdbcClient(carbonStorePath: String) = {
    val logger: Logger = LoggerFactory.getLogger("Presto Server on CarbonData")

    import scala.collection.JavaConverters._

    val prestoProperties: util.Map[String, String] = Map(("http-server.http.port", "8086")).asJava

    logger.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = createQueryRunner(
      prestoProperties, carbonStorePath)

    logger.info("STARTED SERVER AT :" + queryRunner.getCoordinator.getBaseUrl)
  }

  // Instantiates the Presto Server to connect with the Apache CarbonData
  @throws[Exception]
  def createQueryRunner(extraProperties: util.Map[String, String], carbonStorePath: String): DistributedQueryRunner = {
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

  def executeQuery(query: String): util.List[util.Map[String, AnyRef]] = {

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

  def resultSetToList(rs: ResultSet): util.List[util.Map[String, AnyRef]] = {
    val metaData = rs.getMetaData
    val columns = metaData.getColumnCount
    val rows = new util.ArrayList[util.Map[String, AnyRef]]
    while (rs.next) {
      val row = new util.HashMap[String, AnyRef](columns)
      var counter = 1
      while (counter <= columns) {
          row.put(metaData.getColumnName(counter), rs.getObject(counter))
          counter += 1
          counter
      }
      rows.add(row)
    }
    rows
  }
}
