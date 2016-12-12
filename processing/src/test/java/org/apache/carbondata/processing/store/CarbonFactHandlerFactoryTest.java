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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnGroupModel;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class CarbonFactHandlerFactoryTest {

  @Test public void testCreateCarbonFactHandler() {
    CarbonFactHandlerFactory carbonFactHandlerFactory = new CarbonFactHandlerFactory();
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(new ColumnSchema());
    int[] columnCardinality = { 1 };
    carbonFactDataHandlerModel
        .setSegmentProperties(new SegmentProperties(columnSchemas, columnCardinality));
    carbonFactDataHandlerModel.setTableName("TableName");
    carbonFactDataHandlerModel.setDimensionCount(1);
    Map<Integer, GenericDataType> complexIndexMap = new HashMap();
    carbonFactDataHandlerModel.setComplexIndexMap(complexIndexMap);
    new MockUp<SegmentProperties>() {
      @Mock public ColumnGroupModel getColumnGroupModel() {
        return new ColumnGroupModel();
      }

      @Mock public int[] getDimColumnsCardinality() {
        int[] dimColsCardinality = { 1 };
        return dimColsCardinality;
      }
    };
    new MockUp<CarbonUtil>() {
      @Mock public boolean[] identifyDimensionType(List<CarbonDimension> tableDimensionList) {
        boolean[] dictColumns = { true };
        return dictColumns;
      }
    };

    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return new CarbonTable();
      }
    };
    new MockUp<CarbonTable>() {
      @Mock public List<CarbonDimension> getDimensionByTableName(String tableName) {
        List<CarbonDimension> carbonDimensions = new ArrayList<>();
        carbonDimensions.add(new CarbonDimension(new ColumnSchema(), 1, 1, 1, 1));
        return carbonDimensions;
      }
    };

    new MockUp<ColumnGroupModel>() {
      @Mock public int[] getColumnSplit() {
        int[] colSplits = { 1 };
        return colSplits;
      }

      @Mock public boolean isColumnar(int colGroup) {
        return true;
      }

      @Mock public int getNoOfColumnStore() {
        return 1;
      }
    };
    CarbonFactHandlerFactory.FactHandlerType factHandlerType =
        CarbonFactHandlerFactory.FactHandlerType.COLUMNAR;
    CarbonFactHandler expectedResult = carbonFactHandlerFactory
        .createCarbonFactHandler(carbonFactDataHandlerModel, factHandlerType);
    assertNotNull(expectedResult);
  }

}
