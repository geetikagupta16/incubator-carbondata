package org.apache.carbondata.presto.it.datatype

import java.io.File

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import util.CarbonDataStoreCreator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.presto.server.PrestoServer


class AllDataTypeTest extends FunSuiteLike with BeforeAndAfterAll {
  private val logger = LogServiceFactory
    .getLogService(classOf[AllDataTypeTest].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"

  override def beforeAll: Unit = {
    CarbonDataStoreCreator
      .createCarbonStore(storePath, s"$rootPath/integration/presto/src/test/resources/data.csv")
    logger.info(s"\nCarbon store is created at location: $storePath")
    PrestoServer.startServer(storePath)
  }

  test("select double data type with ORDER BY and group by clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT DISTINCT SALARY FROM TESTDB.TESTTABLE ORDER BY SALARY ")
    val expectedResult: List[Map[String, Any]] = List(Map("SALARY" -> 15002.11),
      Map("SALARY" -> 15003.0),
      Map("SALARY" -> 15004.0),
      Map("SALARY" -> 15005.0),
      Map("SALARY" -> 15006.0),
      Map("SALARY" -> 15007.5),
      Map("SALARY" -> 15008.0),
      Map("SALARY" -> 150010.999),
      Map("SALARY" -> 5000000.0))
    assert(actualResult.equals(expectedResult))
  }
  test("select decimal data type with ORDER BY  clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT DISTINCT BONUS FROM TESTDB.TESTTABLE ORDER BY BONUS limit 3 ")
    val expectedResult: List[Map[String, Any]] = List(Map(
      "BONUS" -> BigDecimal.valueOf(500.41).setScale(2)),
      Map("BONUS" -> BigDecimal.valueOf(500.59).setScale(2)),
      Map("BONUS" -> BigDecimal.valueOf(500.88).setScale(2)))
    assert(actualResult.flatten.mkString.equals(expectedResult.flatten.mkString))
  }
  test("select string type with order by clause"){
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT name from testdb.testtable order by name ")
    val expectedResult: List[Map[String, Any]] = List(Map("name" -> "akash"), Map("name" -> "anubhav"), Map("name" -> "bhavya"), Map("name" -> "geetika"), Map("name" -> "jatin"), Map("name" -> "jitesh"), Map("name" -> "liang"), Map("name" -> "prince"), Map("name" -> "ravindra"), Map("name" -> "sahil"))
    assert(actualResult.equals(expectedResult))

  }
  test("select date type with order by clause"){
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT date from testdb.testtable")
    val expectedResult: List[Map[String, Any]] = List(Map("name" -> "akash"), Map("name" -> "anubhav"), Map("name" -> "bhavya"), Map("name" -> "geetika"), Map("name" -> "jatin"), Map("name" -> "jitesh"), Map("name" -> "liang"), Map("name" -> "prince"), Map("name" -> "ravindra"), Map("name" -> "sahil"))
    assert(actualResult.equals(expectedResult))

  }
  override def afterAll: Unit = {
    PrestoServer.stopServer()
  }
}
