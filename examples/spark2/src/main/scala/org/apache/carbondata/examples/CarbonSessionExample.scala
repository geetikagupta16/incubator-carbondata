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

package org.apache.carbondata.examples

import java.io.File

object CarbonSessionExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("drop table if exists uniqdata_timestamp")
    spark.sql("CREATE TABLE uniqdata_timestamp (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION " +
              "string, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1" +
              " decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, " +
              "Double_COLUMN2 double, INTEGER_COLUMN1 int) partitioned by(dob timestamp) STORED " +
              "BY 'org.apache.carbondata.format' TBLPROPERTIES (\"TABLE_BLOCKSIZE\"= \"256 MB\")").show


spark.sql("LOAD DATA INPATH 'hdfs://localhost:54311/Files/2000_UniqData.csv' into table uniqdata_timestamp partition(dob='fdsgfd') OPTIONS ('FILEHEADER'='CUST_ID,CUST_NAME ,ACTIVE_EMUI_VERSION,DOB,DOJ, BIGINT_COLUMN1, BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1, Double_COLUMN2,INTEGER_COLUMN1','BAD_RECORDS_ACTION'='FORCE')")

    spark.sql("select count(*) from uniqdata_timestamp").show
    /*spark.sql("DROP TABLE IF EXISTS carbon_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table
         | WHERE stringfield = 'spark' AND decimalField > 40
      """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table WHERE length(stringField) = 5
       """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table WHERE date_format(dateField, "yyyy-MM-dd") = "2015-07-23"
       """.stripMargin).show()

    spark.sql("SELECT count(stringField) FROM carbon_table").show()

    spark.sql(
      s"""
         | SELECT sum(intField), stringField
         | FROM carbon_table
         | GROUP BY stringField
       """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT t1.*, t2.*
         | FROM carbon_table t1, carbon_table t2
         | WHERE t1.stringField = t2.stringField
      """.stripMargin).show()

    spark.sql(
      s"""
         | WITH t1 AS (
         | SELECT * FROM carbon_table
         | UNION ALL
         | SELECT * FROM carbon_table
         | )
         | SELECT t1.*, t2.*
         | FROM t1, carbon_table t2
         | WHERE t1.stringField = t2.stringField
      """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table
         | WHERE stringField = 'spark' and floatField > 2.8
       """.stripMargin).show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
*/
    spark.stop()
  }

}