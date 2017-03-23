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

import org.apache.carbondata.examples.util.ExampleUtils;
import org.apache.carbondata.hadoop.CarbonInputFormat;
import org.apache.carbondata.hadoop.CarbonProjection;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.CarbonContext;

/**
 * Created by knoldus on 22/3/17.
 */
public class PrestoExample {

  public static void main(String args[]) {

    CarbonContext cc = ExampleUtils.createCarbonContext("PrestoExample");
    String path = ExampleUtils.writeSampleCarbonFile(cc, "carbon1", 1000);
    CarbonProjection projection = new CarbonProjection();
    projection.addColumn("c1");  // column c1
    projection.addColumn("c3");  // column c3
    Configuration conf = new Configuration();
    CarbonInputFormat.setColumnProjection(conf, projection);

    //val env = ExecutionEnvironment.getExecutionEnvironment
  }

}
