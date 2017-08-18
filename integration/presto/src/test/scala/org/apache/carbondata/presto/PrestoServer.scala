package org.apache.carbondata.presto

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, SQLException}
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

  //@throws[Exception]
  /* def main(args: Array[String]): Unit = {
     val rootFilePath = s"$rootPath/integration/presto/data/"

     System.gc()

     val carbonStorePath = CARBONDATA_STOREPATH
     prestoJdbcClient(carbonStorePath)
    /* prestoJdbcClient(carbonStorePath).foreach { (x: Array[Double]) =>
       val y: List[Double] = (x map { (z: Double) => z }).toList
       (BenchMarkingUtil.queries, y).zipped foreach { (query, time) =>

         println(">>>>>>QUERY EXECUTION TIME " + time)
         // util.writeResults(s"\n$time",prestoFile)
         util.writeResults(" [ Query :" + query + "\n"
           + "Time :" + time + " ] \n\n "
           , prestoFile)
       }
     }*/
     // scalastyle:off
     // util.readFromFile(prestoFile).foreach(line => println(line))
     // scalastyle:on
     System.exit(0)
   }
 */
  // Creates a JDBC Client to connect CarbonData to Presto
  @throws[Exception]
  def prestoJdbcClient(carbonStorePath: String) = {
    val logger: Logger = LoggerFactory.getLogger("Presto Server on CarbonData")

    import scala.collection.JavaConverters._

    val prestoProperties: util.Map[String, String] = Map(("http-server.http.port", "8086")).asJava

    logger.info("======== STARTING PRESTO SERVER ========")
    val queryRunner: DistributedQueryRunner = createQueryRunner(
      prestoProperties, carbonStorePath)
    /*    val queryRunner: DistributedQueryRunner = createQueryRunner(
          ImmutableMap.of("http-server.http.port", "8086"))*/
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
      // val stmt: Statement = conn.createStatement

      /* val executionTime: Array[(Query, util.List[util.Map[String, AnyRef]])] = BenchMarkingUtil.queries.map { queries =>
           val res: ResultSet = stmt.executeQuery(queries.sqlText)
         (queries,resultSetToList(res))
         }*/
      /*val colCount: Int = res.getMetaData.getColumnCount
       val x = new Array[Any](res.getRow)
        x.zipWithIndex.map { data =>
          val (index, arrData) = data
          if(res.next()) {
            //println(res.getInt("count") + "------" + counter)
            for (col <- 1 to colCount) {
              val colData = res.getString(col)
            }
          }
        }
        println("row count ----" )
      }*/


      conn.close()
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

  def resultSetToList(rs: ResultSet): util.List[util.Map[String, AnyRef]] = {
    val metaData = rs.getMetaData
    val columns = metaData.getColumnCount
    val rows = new util.ArrayList[util.Map[String, AnyRef]]
    while (rs.next) {
      val row = new util.HashMap[String, AnyRef](columns)
      var counter = 1
      while (counter <= columns) {
        {
          row.put(metaData.getColumnName(counter), rs.getObject(counter))
        }
        {
          counter += 1;
          counter
        }
      }
      rows.add(row)
    }
    rows
  }
}
