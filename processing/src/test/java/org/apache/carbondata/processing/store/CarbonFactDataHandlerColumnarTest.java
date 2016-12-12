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
package org.apache.carbondata.processing.store;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.tools.cmd.gen.AnyVals;

public class CarbonFactDataHandlerColumnarTest {

  private static SegmentProperties segmentProperties;
  private static  List<ColumnSchema> columnSchemas = new ArrayList<>();

  @BeforeClass
  public static void setUp(){

    final List<CarbonDimension> carbonDimensions =new ArrayList<>();
    carbonDimensions.add(new CarbonDimension(new ColumnSchema(), 1,1,1,1));
    new MockUp<CarbonFactDataHandlerModel>() {
      int[] columnCardinalities = {1};

      @Mock public SegmentProperties getSegmentProperties(){
        columnSchemas.add(new ColumnSchema());
        segmentProperties = new SegmentProperties(columnSchemas, columnCardinalities);
        return segmentProperties;
      }

      @Mock public String getTableName() { return "tableName";}
    };

    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return carbonDimensions;
      }
    };
  }

  @Test public void testAddDataToStore() {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    CarbonFactDataHandlerColumnar carbonFactDataHandlerColumnar = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
    Object[] rows = {new Object()};

    new MockUp<CarbonFactDataHandlerModel>() {
      @Mock public SegmentProperties getSegmentProperties(){
        return segmentProperties;
      }
    };
    carbonFactDataHandlerColumnar.addDataToStore(rows);

  }

}
