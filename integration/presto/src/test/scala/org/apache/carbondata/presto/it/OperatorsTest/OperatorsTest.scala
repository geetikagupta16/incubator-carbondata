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
package org.apache.carbondata.presto.it.OperatorsTest

import java.io.File

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import util.CarbonDataStoreCreator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.presto.server.PrestoServer

class OperatorsTest extends FunSuiteLike with BeforeAndAfterAll {
  private val logger = LogServiceFactory
    .getLogService(classOf[OperatorsTest].getCanonicalName)
  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"

  override def beforeAll: Unit = {
    CarbonDataStoreCreator
      .createCarbonStore(storePath, s"$rootPath/integration/presto/src/test/resources/data.csv")
    logger.info(s"\nCarbon store is created at location: $storePath")
    PrestoServer.startServer(storePath)
  }

  test("test and filter clause on all data types") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery(
        "SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE " +
        "WHERE BONUS>1234 AND ID>2 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," +
        "BONUS ORDER BY ID")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 4,
      "NAME" -> "prince",
      "BONUS" -> java.math.BigDecimal.valueOf(9999.9990).setScale(4),
      "COUNTRY" -> "china",
      "SALARY" -> 15003.0,
      "DATE" -> "1970-01-03",
      "SERIALNAME" -> "ASD66902",
      "PHONETYPE" -> "phone2435"),
      Map("ID" -> 5,
        "NAME" -> "bhavya",
        "BONUS" -> java.math.BigDecimal.valueOf(5000.999).setScale(4),
        "COUNTRY" -> "china",
        "SALARY" -> 15004.0,
        "DATE" -> "1970-01-04",
        "SERIALNAME" -> "ASD90633",
        "PHONETYPE" -> "phone2441"))
    assert(actualResult.toString() equals expectedResult.toString())


  }

  test("test greater than operator on DATE data type value") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery(
        "SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE " +
        "WHERE BONUS>1234 AND ID>2 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," +
        "BONUS ORDER BY ID")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 4,
      "NAME" -> "prince",
      "BONUS" -> java.math.BigDecimal.valueOf(9999.9990).setScale(4),
      "COUNTRY" -> "china",
      "SALARY" -> 15003.0,
      "DATE" -> "1970-01-03",
      "SERIALNAME" -> "ASD66902",
      "PHONETYPE" -> "phone2435"),
      Map("ID" -> 5,
        "NAME" -> "bhavya",
        "BONUS" -> java.math.BigDecimal.valueOf(5000.999).setScale(4),
        "COUNTRY" -> "china",
        "SALARY" -> 15004.0,
        "DATE" -> "1970-01-04",
        "SERIALNAME" -> "ASD90633",
        "PHONETYPE" -> "phone2441"))
    assert(actualResult.toString().equals(expectedResult.toString()))


  }
  test("test less than operator on DATE data type value") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery(
        "SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE " +
        "WHERE BONUS>1234 AND ID<2 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," +
        "BONUS ORDER BY ID")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1,
      "NAME" -> "anubhav",
      "BONUS" -> java.math.BigDecimal.valueOf(1234.4440).setScale(4),
      "DATE" -> "1970-01-09",
      "SALARY" -> 5000000.0,
      "SERIALNAME" -> "ASD69643",
      "COUNTRY" -> "china",
      "PHONETYPE" -> "phone197"))
    assert(actualResult.toString().equals(expectedResult.toString()))


  }
  test("test the result for in clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT NAME from testdb.testtable WHERE PHONETYPE IN('phone1848','phone706')")
    val expectedResult: List[Map[String, Any]] = List(
      Map("NAME" -> "geetika"),
      Map("NAME" -> "ravindra"),
      Map("NAME" -> "jitesh"))

    assert(actualResult.equals(expectedResult))
  }

  override def afterAll: Unit = {
    PrestoServer.stopServer()
  }
}
