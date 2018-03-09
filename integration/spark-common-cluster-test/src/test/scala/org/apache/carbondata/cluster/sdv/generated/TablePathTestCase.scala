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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

class TablePathTestCase extends QueryTest with BeforeAndAfterAll{

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS EMPLOYEE")
    sql("DROP TABLE IF EXISTS EMPLOYEE1")
  }
  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS EMPLOYEE")
    sql("DROP TABLE IF EXISTS EMPLOYEE1")

  }

  //User must be able to create table with path
  test("Table_Location_TC_01",Include){
   sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE(ID INT)STORED BY 'CARBONDATA' LOCATION '${TestQueryExecutor.hdfsUrl}' ")
    checkExistence(sql("SHOW TABLES"),true,"employee")
  }
  //Create Two Table at same locations
  test("Table_Location_TC_02",Include){
    sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE(ID INT)STORED BY 'CARBONDATA' LOCATION '${TestQueryExecutor.hdfsUrl}' ")
    sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE1(ID INT)STORED BY 'CARBONDATA' LOCATION '${TestQueryExecutor.hdfsUrl}' ")
    val result = sql("SHOW TABLES")
    checkExistence(result,true,"employee")
    checkExistence(result,true,"employee1")

  }
  //drop one table other should also get deleted
  test("Table_Location_TC_03",Include){
    sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE(ID INT)STORED BY 'CARBONDATA' LOCATION '${TestQueryExecutor.hdfsUrl}' ")
    sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE1(ID INT)STORED BY 'CARBONDATA' LOCATION '${TestQueryExecutor.hdfsUrl}' ")
    sql("DROP TABLE IF EXISTS EMPLOYEE")
    sql("DROP TABLE IF EXISTS EMPLOYEE1")
    val result = sql("SHOW TABLES")
    checkExistence(result,false,"employee")
    checkExistence(result,false,"employee1")

  }
  //load data inside the table
  test("Table_Location_TC_04",Include){
    sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE(ID INT)STORED BY 'CARBONDATA' LOCATION '${TestQueryExecutor.hdfsUrl}' ")
    sql("INSERT INTO EMPLOYEE VALUES(1)")
    val result = sql("SELECT * FROM EMPLOYEE")
    checkAnswer(result,Seq(Row(1)))
  }

  //create table with dictionary_include column
  test("Table_Location_TC_05",Include){
    sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE(ID INT)STORED BY 'CARBONDATA'  LOCATION '${TestQueryExecutor.hdfsUrl}' TblProperties('dictionary_include'='id') ")
    checkExistence(sql("SHOW TABLES"),true,"employee")
  }
  //create table with dictionary_exclude column
  test("Table_Location_TC_06",Include){
    sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE(ID INT)STORED BY 'CARBONDATA'  LOCATION '${TestQueryExecutor.hdfsUrl}' TblProperties('dictionary_exclude'='id') ")
    checkExistence(sql("SHOW TABLES"),true,"employee")
  }
  //create table with sort column
  test("Table_Location_TC_07",Include){
    sql(s"CREATE TABLE IF NOT EXISTS EMPLOYEE(ID INT)STORED BY 'CARBONDATA'  LOCATION '${TestQueryExecutor.hdfsUrl}' TblProperties('sort_columns'='id') ")
    sql("INSERT INTO EMPLOYEE VALUES(3)")
    sql("INSERT INTO EMPLOYEE VALUES(12)")
    sql("INSERT INTO EMPLOYEE VALUES(1)")

    checkAnswer(sql("SELECT * FROM EMPLOYEE"),Seq(Row(1),Row(3),Row(12)))
  }

}
