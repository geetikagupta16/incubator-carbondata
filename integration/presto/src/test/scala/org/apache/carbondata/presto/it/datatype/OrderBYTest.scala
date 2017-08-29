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
package org.apache.carbondata.presto.it.datatype

import java.io.File

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import util.CarbonDataStoreCreator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.presto.server.PrestoServer


class OrderBYTest extends FunSuiteLike with BeforeAndAfterAll {
  private val logger = LogServiceFactory
    .getLogService(classOf[OrderBYTest].getCanonicalName)
  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"

  override def beforeAll: Unit = {
    CarbonDataStoreCreator
      .createCarbonStore(storePath, s"$rootPath/integration/presto/src/test/resources/data.csv")
    logger.info(s"\nCarbon store is created at location: $storePath")
    PrestoServer.startServer(storePath)
  }

  test("select double data type with ORDER BY clause") {
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
      "BONUS" -> java.math.BigDecimal.valueOf(500.414).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.59).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.88).setScale(4)))
    println("actual result" + actualResult + "expectedresult" + expectedResult)
    assert(actualResult.equals(expectedResult))
  }
  test("select string type with order by clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT NAME FROM TESTDB.TESTTABLE ORDER BY NAME")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "akash"),
      Map("NAME" -> "anubhav"),
      Map("NAME" -> "bhavya"),
      Map("NAME" -> "geetika"),
      Map("NAME" -> "jatin"),
      Map("NAME" -> "jitesh"),
      Map("NAME" -> "liang"),
      Map("NAME" -> "prince"),
      Map("NAME" -> "ravindra"),
      Map("NAME" -> "sahil"))
    assert(actualResult.equals(expectedResult))
  }
  test("select DATE type with order by clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT DATE FROM TESTDB.TESTTABLE ORDER BY DATE")
    val expectedResult: List[Map[String, Any]] = List(Map("DATE" -> "1970-01-02"),
      Map("DATE" -> "1970-01-03"),
      Map("DATE" -> "1970-01-04"),
      Map("DATE" -> "1970-01-05"),
      Map("DATE" -> "1970-01-06"),
      Map("DATE" -> "1970-01-07"),
      Map("DATE" -> "1970-01-07"),
      Map("DATE" -> "1970-01-08"),
      Map("DATE" -> "1970-01-09"),
      Map("DATE" -> "1970-01-10"))

    assert(actualResult.toString().equals(expectedResult.toString()))

  }
  test("select int type with order by clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT DISTINCT ID FROM TESTDB.TESTTABLE ORDER BY ID")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1),
      Map("ID" -> 2),
      Map("ID" -> 3),
      Map("ID" -> 4),
      Map("ID" -> 5),
      Map("ID" -> 6),
      Map("ID" -> 7),
      Map("ID" -> 8),
      Map("ID" -> 9))

    assert(actualResult.toString().equals(expectedResult.toString()))

  }

  override def afterAll: Unit = {
    PrestoServer.stopServer()
  }


}
