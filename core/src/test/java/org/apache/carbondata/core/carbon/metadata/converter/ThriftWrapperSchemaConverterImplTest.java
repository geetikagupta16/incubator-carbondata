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
package org.apache.carbondata.core.carbon.metadata.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.carbon.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.carbon.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ThriftWrapperSchemaConverterImplTest {

  private static ThriftWrapperSchemaConverterImpl thriftWrapperSchemaConverter = null;
  private static org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolEntry;
  private static org.apache.carbondata.format.ColumnSchema thriftColumnSchema = null;
  private static List<ColumnSchema> columnSchemas = null;
  private static List<SchemaEvolutionEntry> schemaEvolutionEntries = null;

  @BeforeClass public static void setUp() {
    thriftWrapperSchemaConverter =
        new ThriftWrapperSchemaConverterImpl();

    schemaEvolEntry =
        new org.apache.carbondata.format.SchemaEvolutionEntry();

     schemaEvolutionEntries = new ArrayList();
    schemaEvolutionEntries.add(new SchemaEvolutionEntry());

    columnSchemas = new ArrayList();
    columnSchemas.add(new ColumnSchema());
    final List<Encoding> encodings = new ArrayList<>();
    encodings.add(Encoding.INVERTED_INDEX);
    encodings.add(Encoding.DELTA);
    encodings.add(Encoding.BIT_PACKED);
    encodings.add(Encoding.DICTIONARY);
    encodings.add(Encoding.RLE);
    encodings.add(Encoding.DIRECT_DICTIONARY);

    List<org.apache.carbondata.format.Encoding> encoders =
        new ArrayList<org.apache.carbondata.format.Encoding>();
    encoders.add(org.apache.carbondata.format.Encoding.INVERTED_INDEX);
    encoders.add(org.apache.carbondata.format.Encoding.DELTA);
    encoders.add(org.apache.carbondata.format.Encoding.BIT_PACKED);
    encoders.add(org.apache.carbondata.format.Encoding.DICTIONARY);
    encoders.add(org.apache.carbondata.format.Encoding.RLE);
    encoders.add(org.apache.carbondata.format.Encoding.DIRECT_DICTIONARY);

    final Map map = new HashMap<String, String>();
    map.put("property", "value");
    thriftColumnSchema = new org.apache.carbondata.format.ColumnSchema(org.apache.carbondata.format.DataType.STRING,
            "columnName", "1", true, encoders, true);

    new MockUp<ColumnSchema>() {

      @Mock public List<Encoding> getEncodingList() {
        return encodings;
      }

      @Mock public DataType getDataType() {
        return DataType.BOOLEAN;
      }

      @Mock public String getColumnName() {
        return "columnName";
      }

      @Mock public String getColumnUniqueId() {
        return "1";
      }

      @Mock public boolean isColumnar() {
        return true;
      }

      @Mock public boolean isDimensionColumn() {
        return true;
      }

      @Mock public int getColumnGroupId() {
        return 1;
      }

      @Mock public int getScale() {
        return 1;
      }

      @Mock public int getPrecision() {
        return 1;
      }

      @Mock public int getNumberOfChild() {
        return 1;
      }

      @Mock public byte[] getDefaultValue() {
        return new byte[] { 1, 2 };
      }

      @Mock public Map<String, String> getColumnProperties() {
        return map;
      }

      @Mock public boolean isInvisible() {
        return true;
      }

      @Mock public String getColumnReferenceId() {
        return "1";
      }
    };

    new MockUp<SchemaEvolutionEntry>() {
      @Mock public List<ColumnSchema> getAdded() {
        return columnSchemas;
      }

      @Mock public List<ColumnSchema> getRemoved() {
        return columnSchemas;
      }

    };

    new MockUp<org.apache.carbondata.format.SchemaEvolutionEntry>() {
      @Mock public org.apache.carbondata.format.SchemaEvolutionEntry setAdded(
          List<org.apache.carbondata.format.ColumnSchema> added) {
        return schemaEvolEntry;
      }

      @Mock public org.apache.carbondata.format.SchemaEvolutionEntry setRemoved(
          List<org.apache.carbondata.format.ColumnSchema> removed) {
        return schemaEvolEntry;
      }
    };
    new MockUp<org.apache.carbondata.format.ColumnSchema>() {
      @Mock
      public org.apache.carbondata.format.ColumnSchema setColumn_group_id(int column_group_id) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setScale(int scale) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setPrecision(int precision) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setNum_child(int num_child) {
        return thriftColumnSchema;
      }

      @Mock
      public org.apache.carbondata.format.ColumnSchema setDefault_value(byte[] default_value) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setColumnProperties(
          Map<String, String> columnProperties) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setInvisible(boolean invisible) {
        return thriftColumnSchema;
      }

      @Mock public org.apache.carbondata.format.ColumnSchema setColumnReferenceId(
          String columnReferenceId) {
        return thriftColumnSchema;
      }
    };

    new MockUp<SchemaEvolution>() {
      @Mock public List<SchemaEvolutionEntry> getSchemaEvolutionEntryList() {
        return schemaEvolutionEntries;
      }
    };

  }

  @Test public void testFromWrapperToExternalSchemaEvolutionEntry() {
    final SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
    org.apache.carbondata.format.SchemaEvolutionEntry actualResult = thriftWrapperSchemaConverter
        .fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry);
    assertEquals(schemaEvolEntry, actualResult);
  }

  @Test public void testFromWrapperToExternalSchemaEvolution() {
    SchemaEvolution schemaEvolution = new SchemaEvolution();

    List<org.apache.carbondata.format.SchemaEvolutionEntry> thriftSchemaEvolutionEntries =
        new ArrayList<>();
    thriftSchemaEvolutionEntries.add(schemaEvolEntry);

    org.apache.carbondata.format.SchemaEvolution actualResult =
        thriftWrapperSchemaConverter.fromWrapperToExternalSchemaEvolution(schemaEvolution);

    org.apache.carbondata.format.SchemaEvolution expectedResult =
        new org.apache.carbondata.format.SchemaEvolution(thriftSchemaEvolutionEntries);
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testFromWrapperToExternalColumnSchema() {
    ColumnSchema wrapperColumnSchema = new ColumnSchema();
    org.apache.carbondata.format.ColumnSchema actualResult = thriftWrapperSchemaConverter.fromWrapperToExternalColumnSchema(wrapperColumnSchema);
    assertEquals(thriftColumnSchema, actualResult);
  }

  @Test public void testFromWrapperToExternalTableSchema() {
    TableSchema wrapperTableSchema = new TableSchema();
    final org.apache.carbondata.format.TableSchema tabSchema = new org.apache.carbondata.format.TableSchema();
    final SchemaEvolution schemaEvolution = new SchemaEvolution();
    final Map map = new HashMap<String, String>();
    new MockUp<TableSchema>() {
      @Mock public List<ColumnSchema> getListOfColumns() {return columnSchemas;}
      @Mock public SchemaEvolution getSchemaEvalution() { return schemaEvolution;}
      @Mock public String getTableId() { return "tableId";}
      @Mock public Map<String, String> getTableProperties() { return map;}
    };

    new MockUp<org.apache.carbondata.format.TableSchema>() {
      @Mock public org.apache.carbondata.format.TableSchema setTableProperties(Map<String, String> tableProperties) { return tabSchema; }
    };

    List<org.apache.carbondata.format.SchemaEvolutionEntry> thriftSchemaEvolutionEntries =
        new ArrayList<>();
    thriftSchemaEvolutionEntries.add(schemaEvolEntry);
    org.apache.carbondata.format.SchemaEvolution schemaEvol =
        new org.apache.carbondata.format.SchemaEvolution(thriftSchemaEvolutionEntries);
    List<org.apache.carbondata.format.ColumnSchema> thriftColumnSchemas = new ArrayList<org.apache.carbondata.format.ColumnSchema>();
    thriftColumnSchemas.add(thriftColumnSchema);
    org.apache.carbondata.format.TableSchema expectedResult =
        new org.apache.carbondata.format.TableSchema("tableId", thriftColumnSchemas, schemaEvol);
    org.apache.carbondata.format.TableSchema actualResult = thriftWrapperSchemaConverter.fromWrapperToExternalTableSchema(wrapperTableSchema);
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testFromWrapperToExternalTableInfo() {

  }

}
