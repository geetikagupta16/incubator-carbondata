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

import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnarFormatVersion;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.store.writer.CarbonDataWriterVo;
import org.apache.carbondata.processing.store.writer.CarbonFactDataWriter;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class CarbonDataWriterFactoryTest {

  static CarbonDataWriterVo carbonDataWriterVo = null;
  static CarbonDataWriterFactory carbonDataWriterFactory = null;

  @BeforeClass public static void setUp() {

    carbonDataWriterFactory = CarbonDataWriterFactory.getInstance();
    carbonDataWriterVo = new CarbonDataWriterVo();
    carbonDataWriterVo.setTableName("tableName");
    carbonDataWriterVo.setStoreLocation("/storeLocation");
    List<ColumnSchema> colSchemas = new ArrayList<>();
    colSchemas.add(new ColumnSchema());
    carbonDataWriterVo.setWrapperColumnSchemaList(colSchemas);
    int[] colCardinality = { 1 };
    carbonDataWriterVo.setColCardinality(colCardinality);
  }

  @Test public void testGetFactDataWriter() {
    new MockUp<CarbonTable>() {
      @Mock public CarbonTableIdentifier getCarbonTableIdentifier() {
        return new CarbonTableIdentifier("databaseName", "tableName", "tableId");
      }
    };

    new MockUp<CarbonUtil>() {
      @Mock public boolean hasEncoding(List<Encoding> encodings, Encoding encoding) {
        return true;
      }
    };

    new MockUp<ThriftWrapperSchemaConverterImpl>() {
      @Mock public org.apache.carbondata.format.ColumnSchema fromWrapperToExternalColumnSchema(
          ColumnSchema wrapperColumnSchema) {
        return new org.apache.carbondata.format.ColumnSchema();
      }
    };
    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return new CarbonTable();
      }
    };

    CarbonFactDataWriter actualResult =
        carbonDataWriterFactory.getFactDataWriter(ColumnarFormatVersion.V1, carbonDataWriterVo);
    assertNotNull(actualResult);
  }

  @Test public void testGetFactDataWriterForVersionV2() {
    new MockUp<CarbonTable>() {
      @Mock public CarbonTableIdentifier getCarbonTableIdentifier() {
        return new CarbonTableIdentifier("databaseName", "tableName", "tableId");
      }
    };

    new MockUp<CarbonUtil>() {
      @Mock public boolean hasEncoding(List<Encoding> encodings, Encoding encoding) {
        return true;
      }
    };

    new MockUp<ThriftWrapperSchemaConverterImpl>() {
      @Mock public org.apache.carbondata.format.ColumnSchema fromWrapperToExternalColumnSchema(
          ColumnSchema wrapperColumnSchema) {
        return new org.apache.carbondata.format.ColumnSchema();
      }
    };
    new MockUp<CarbonMetadata>() {
      @Mock public CarbonTable getCarbonTable(String tableUniqueName) {
        return new CarbonTable();
      }
    };

    CarbonFactDataWriter actualResult =
        carbonDataWriterFactory.getFactDataWriter(ColumnarFormatVersion.V2, carbonDataWriterVo);
    assertNotNull(actualResult);
  }

}
