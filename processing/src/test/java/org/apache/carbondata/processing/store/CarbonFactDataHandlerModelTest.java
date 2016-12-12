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

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CarbonFactDataHandlerModelTest {
  CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();

  @Test public void testCreateCarbonFactDataHandlerModel() {

    final CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier("databaseName", "tableName", "tableId");
    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public AbsoluteTableIdentifier getTableIdentifier() {
        return new AbsoluteTableIdentifier("/store/location", carbonTableIdentifier);
      }
    };

    new MockUp<CarbonDataProcessorUtil>() {
      @Mock public boolean[] getIsUseInvertedIndex(DataField[] fields) {
        boolean[] isUseInvertedIndex = { true };
        return isUseInvertedIndex;
      }

      @Mock public Map<String, GenericDataType> getComplexTypesMap(DataField[] dataFields) {
        Map<String, GenericDataType> complexTypeMap = new HashMap();
        complexTypeMap.put("", new PrimitiveDataType("name", "parentName", "1", 1));
        return complexTypeMap;
      }
    };

    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public int getDimensionCount() {
        return 1;
      }

      @Mock public int getNoDictionaryCount() {
        return 0;
      }

      @Mock public int getComplexDimensionCount() {
        return 0;
      }

      @Mock public int getMeasureCount() {
        return 0;
      }
    };

    new MockUp<CarbonUtil>() {
      @Mock
      public List<ColumnSchema> getColumnSchemaList(List<CarbonDimension> carbonDimensionsList,
          List<CarbonMeasure> carbonMeasureList) {
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(new ColumnSchema());
        return columnSchemas;
      }

      @Mock public int[] getFormattedCardinality(int[] dictionaryColumnCardinality,
          List<ColumnSchema> wrapperColumnSchemaList) {
        int[] cardinality = { 1 };
        return cardinality;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public CarbonTableIdentifier getCarbonTableIdentifier() {
        return carbonTableIdentifier;
      }
    };

    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return new CarbonTable();
      }
    };

    int[] dimLengths = { 1 };
    CarbonDataLoadConfiguration configuration = new CarbonDataLoadConfiguration();
    configuration.setTaskNo("1");
    configuration.setDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP, "FactTimeStamp");
    configuration.setDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS, dimLengths);
    CarbonFactDataHandlerModel actualResult = carbonFactDataHandlerModel
        .createCarbonFactDataHandlerModel(configuration, "/store/location");
    int mdKeyIndex = 0;
    boolean useKettle = false;
    assertEquals(mdKeyIndex, actualResult.getMdKeyIndex());
    assertEquals(useKettle, actualResult.isUseKettle());

  }

  @Test public void testCreateCarbonFactDataHandlerModelForComplextype() {

    final CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier("databaseName", "tableName", "tableId");

    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public AbsoluteTableIdentifier getTableIdentifier() {
        return new AbsoluteTableIdentifier("/store/location", carbonTableIdentifier);
      }
    };

    new MockUp<CarbonDataProcessorUtil>() {
      @Mock public boolean[] getIsUseInvertedIndex(DataField[] fields) {
        boolean[] isUseInvertedIndex = { true };
        return isUseInvertedIndex;
      }

      @Mock public Map<String, GenericDataType> getComplexTypesMap(DataField[] dataFields) {
        Map<String, GenericDataType> complexTypeMap = new HashMap();
        ArrayDataType arrayDataType = new ArrayDataType("array", "array", "1");
        arrayDataType.addChildren(new PrimitiveDataType("int", "array", "1", 1));
        complexTypeMap.put("", arrayDataType);
        return complexTypeMap;
      }
    };

    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public int getDimensionCount() {
        return 1;
      }

      @Mock public int getNoDictionaryCount() {
        return 0;
      }

      @Mock public int getComplexDimensionCount() {
        return 0;
      }

      @Mock public int getMeasureCount() {
        return 0;
      }
    };

    new MockUp<CarbonUtil>() {
      @Mock
      public List<ColumnSchema> getColumnSchemaList(List<CarbonDimension> carbonDimensionsList,
          List<CarbonMeasure> carbonMeasureList) {
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(new ColumnSchema());
        return columnSchemas;
      }

      @Mock public int[] getFormattedCardinality(int[] dictionaryColumnCardinality,
          List<ColumnSchema> wrapperColumnSchemaList) {
        int[] cardinality = { 1 };
        return cardinality;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public CarbonTableIdentifier getCarbonTableIdentifier() {
        return carbonTableIdentifier;
      }
    };

    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return new CarbonTable();
      }
    };

    int[] dimLengths = { 1 };
    CarbonDataLoadConfiguration configuration = new CarbonDataLoadConfiguration();
    configuration.setTaskNo("1");
    configuration.setDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP, "FactTimeStamp");
    configuration.setDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS, dimLengths);
    CarbonFactDataHandlerModel actualResult = carbonFactDataHandlerModel
        .createCarbonFactDataHandlerModel(configuration, "/store/location");
    int mdKeyIndex = 0;
    boolean useKettle = false;
    assertEquals(mdKeyIndex, actualResult.getMdKeyIndex());
    assertEquals(useKettle, actualResult.isUseKettle());

  }

  @Test public void testCreateCarbonFactDataHandlerModelWhenNodictionaryCountIsGreaterThanZero() {

    final CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier("databaseName", "tableName", "tableId");
    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public AbsoluteTableIdentifier getTableIdentifier() {
        return new AbsoluteTableIdentifier("/store/location", carbonTableIdentifier);
      }
    };

    new MockUp<CarbonDataProcessorUtil>() {
      @Mock public boolean[] getIsUseInvertedIndex(DataField[] fields) {
        boolean[] isUseInvertedIndex = { true };
        return isUseInvertedIndex;
      }

      @Mock public Map<String, GenericDataType> getComplexTypesMap(DataField[] dataFields) {
        Map<String, GenericDataType> complexTypeMap = new HashMap();
        complexTypeMap.put("", new PrimitiveDataType("name", "parentName", "1", 1));
        return complexTypeMap;
      }
    };

    new MockUp<CarbonDataLoadConfiguration>() {
      @Mock public int getDimensionCount() {
        return 1;
      }

      @Mock public int getNoDictionaryCount() {
        return 1;
      }

      @Mock public int getComplexDimensionCount() {
        return 0;
      }

      @Mock public int getMeasureCount() {
        return 0;
      }
    };

    new MockUp<CarbonUtil>() {
      @Mock
      public List<ColumnSchema> getColumnSchemaList(List<CarbonDimension> carbonDimensionsList,
          List<CarbonMeasure> carbonMeasureList) {
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(new ColumnSchema());
        return columnSchemas;
      }

      @Mock public int[] getFormattedCardinality(int[] dictionaryColumnCardinality,
          List<ColumnSchema> wrapperColumnSchemaList) {
        int[] cardinality = { 1 };
        return cardinality;
      }
    };

    new MockUp<CarbonTable>() {
      @Mock public CarbonTableIdentifier getCarbonTableIdentifier() {
        return carbonTableIdentifier;
      }
    };

    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return new CarbonTable();
      }
    };

    int[] dimLengths = { 1 };
    CarbonDataLoadConfiguration configuration = new CarbonDataLoadConfiguration();
    configuration.setTaskNo("1");
    configuration.setDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP, "FactTimeStamp");
    configuration.setDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS, dimLengths);
    CarbonFactDataHandlerModel actualResult = carbonFactDataHandlerModel
        .createCarbonFactDataHandlerModel(configuration, "/store/location");
    int mdKeyIndex = 1;
    boolean useKettle = false;
    assertEquals(mdKeyIndex, actualResult.getMdKeyIndex());
    assertEquals(useKettle, actualResult.isUseKettle());

  }

}
