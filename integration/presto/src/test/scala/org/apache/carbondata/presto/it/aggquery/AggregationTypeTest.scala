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
package org.apache.carbondata.presto.it.aggquery

import java.io.File

import org.scalatest.Inspectors._
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, ShouldMatchers}
import util.CarbonDataStoreCreator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.presto.server.PrestoServer


class AggregationTypeTest extends FunSuiteLike with BeforeAndAfterAll with ShouldMatchers{

  private val logger = LogServiceFactory
    .getLogService(classOf[AggregationTypeTest].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"

  override def beforeAll: Unit = {
    CarbonDataStoreCreator
      .createCarbonStore(storePath, s"$rootPath/integration/presto/src/test/resources/data.csv")
    logger.info(s"\nCarbon store is created at location: $storePath")
    PrestoServer.startServer(storePath)
  }

  test("test the result for count(*) in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT COUNT(*) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 10))
    forAll(expectedResult)(actualResult should contain(_))
  }
  test("test the result for count()clause with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT COUNT(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 9))
    forAll(expectedResult)(actualResult should contain(_))

  }
  test("test the result for sum()in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT SUM(ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 54))
    forAll(expectedResult)(actualResult should contain(_))
  }
  test("test the result for sum() wiTh distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT SUM(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 45))
    forAll(expectedResult)(actualResult should contain(_))
  }
  test("test the result for avg() wiTh distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT AVG(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 5))
    forAll(expectedResult)(actualResult should contain(_))
  }
  test("test the result for min() wiTh distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT MIN(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 1))
    forAll(expectedResult)(actualResult should contain(_))
  }
  test("test the result for max() wiTh distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT MAX(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 9))
    forAll(expectedResult)(actualResult should contain(_))
  }
  test("test the result for count()clause with distinct operator on decimal column in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT COUNT(DISTINCT INCENTIVES) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 7))
    actualResult.equals(expectedResult)
  }
  test("test the result for count()clause with out  distinct operator on decimal column in presto")
  {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT COUNT(INCENTIVES) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 10))
    actualResult.equals(expectedResult)
  }
  test("test the result for sum()with out distinct operator for decimal column in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT SUM(DISTINCT INCENTIVES) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 54))
    actualResult.equals(expectedResult)
  }
  test("test the result for sum() with distinct operator for decimal column in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT SUM(DISTINCT INCENTIVES) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 62303))
    assert(
      actualResult.head("RESULT").toString.toInt == expectedResult.head("RESULT").toString.toInt)
  }
  test("test the result for avg() with distinct operator on decimal in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT AVG(DISTINCT INCENTIVES) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 8900))
    actualResult.equals(expectedResult)
  }

  /* test("test the result for min() wiTh distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
       .executeQuery("SELECT MIN(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
   val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 1))
     assert(actualResult equals expectedResult)
   }
   test("test the result for max() wiTh distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
       .executeQuery("SELECT MAX(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
   val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 9))
     val result = assert(actualResult equals expectedResult)
   }*/
  override def afterAll(): Unit = {
    PrestoServer.stopServer()
  }
}
