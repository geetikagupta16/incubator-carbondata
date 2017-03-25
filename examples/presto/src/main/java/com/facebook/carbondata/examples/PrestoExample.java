/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.carbondata.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.facebook.presto.carbondata.CarbondataPlugin;
import com.facebook.presto.spi.connector.ConnectorFactory;

/**
 * Created by knoldus on 22/3/17.
 */
public class PrestoExample {

  public static void main(String args[]) {

    /*CarbonContext cc = ExampleUtils.createCarbonContext("PrestoExample");
    String path = ExampleUtils.writeSampleCarbonFile(cc, "carbon1", 1000);*/

    CarbondataPlugin x = new CarbondataPlugin();
    Iterable<ConnectorFactory> y = x.getConnectorFactories();
    final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    final String DB_URL = "jdbc:presto://19.168.2.188:8080/hive/default";

    //  Database credentials
    final String USER = "username";
    final String PASS = "password";

    Connection conn = null;
    Statement stmt = null;
    try {
      //STEP 2: Register JDBC driver
      Class.forName(JDBC_DRIVER);

      //STEP 3: Open a connection
      conn = DriverManager.getConnection(DB_URL,USER,PASS);
      //conn.setCatalog("hive");


      //STEP 4: Execute a query
      stmt = conn.createStatement();
      String sql;
      sql = "create table test(a String)";
      System.out.println("I m here---"+ conn.getSchema());
      Boolean res = stmt.execute(sql);
      System.out.println("Result==========="+ res);

      //STEP 5: Extract data from result set
      /*while(rs.next()){
        //Retrieve by column name
        int nationkey  = rs.getInt("n_nationkey");
        String name = rs.getString("n_name");
        int regionkey = rs.getInt("n_regionkey");
        String comment = rs.getString("n_comment");

        //Display values
        System.out.println(String.format("%d, %s, %d, %s", nationkey, name, regionkey, comment));
      }*///STEP 6: Clean-up environment
     // rs.close();
      stmt.close();
      conn.close();
    } catch (SQLException se) {
      //Handle errors for JDBC
      se.printStackTrace();
    } catch (Exception e) {
      //Handle errors for Class.forName
      e.printStackTrace();
    } finally {
      //finally block used to close resources
      try {
        if (stmt != null) stmt.close();
      } catch (SQLException se2) {
      }
      try {
        if (conn != null) conn.close();
      } catch (SQLException se) {
        se.printStackTrace();
      }
    }
    System.out.println("Goodbye!");
  }
}

