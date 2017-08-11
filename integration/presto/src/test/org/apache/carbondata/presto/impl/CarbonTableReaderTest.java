package org.apache.carbondata.presto.impl;

import com.facebook.presto.spi.SchemaTableName;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.service.impl.PathFactory;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.TableInfo;
import org.apache.thrift.TBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CarbonTableReaderTest {

    CarbonTableReader carbonTableReader = new CarbonTableReader(new CarbonTableConfig());
    File file = new File("schema");

    @Before
    public void setUp() {
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        file.delete();
    }

    private org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema getColumnarDimensionColumn() {
        org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema dimColumn = new org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("imei");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        List<Encoding> encodeList =
                new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodingList(encodeList);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }


    private org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema getColumnarMeasureColumn() {
        org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema dimColumn = new org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema();
        dimColumn.setColumnName("id");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.INT);
        return dimColumn;
    }

    private TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema();
        List<org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema> columnSchemaList = new ArrayList<org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema>();
        columnSchemaList.add(getColumnarMeasureColumn());
        columnSchemaList.add(getColumnarDimensionColumn());
        tableSchema.setListOfColumns(columnSchemaList);
        tableSchema.setTableName("tableName");
        return tableSchema;
    }

    private org.apache.carbondata.core.metadata.schema.table.TableInfo getTableInfo(long timeStamp) {
        org.apache.carbondata.core.metadata.schema.table.TableInfo info = new org.apache.carbondata.core.metadata.schema.table.TableInfo();
        info.setDatabaseName("schemaName");
        info.setLastUpdatedTime(timeStamp);
        info.setTableUniqueName("schemaName_tableName");
        info.setFactTable(getTableSchema());
        info.setStorePath("storePath");
        return info;
    }

    @Test
    public void getCarbonCacheTest() {

        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.LOCAL;
            }

            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                return new LocalCarbonFile("storePath");
            }

            @Mock
            public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType, int bufferSize)
                    throws IOException {
                return new DataInputStream(new FileInputStream(file));
            }
        };

        new MockUp<LocalCarbonFile>() {
            @Mock
            public CarbonFile[] listFiles() {
                CarbonFile[] carbonFiles = {new LocalCarbonFile("storePath")};
                return carbonFiles;
            }
        };

        new MockUp<PathFactory>() {
            @Mock
            public CarbonTablePath getCarbonTablePath(
                    String storeLocation, CarbonTableIdentifier tableIdentifier) {

                return new CarbonTablePath("storePath", "schemaName", "tableName");
            }
        };

        new MockUp<ThriftReader>() {
            @Mock
            public TBase read() {
                return new TableInfo();
            }
        };

        new MockUp<ThriftWrapperSchemaConverterImpl>() {
            @Mock
            org.apache.carbondata.core.metadata.schema.table.TableInfo fromExternalToWrapperTableInfo(org.apache.carbondata.format.TableInfo externalTableInfo,
                                                                                                      String dbName, String tableName, String storePath) {
                return getTableInfo(1000L);
            }
        };

        CarbonTableCacheModel carbonTableCacheModel = carbonTableReader.getCarbonCache(new SchemaTableName("schemaName", "tableName"));
        assert (carbonTableCacheModel.tableInfo.equals(getTableInfo(1000L)));
        assert (carbonTableCacheModel.carbonTablePath.equals(new CarbonTablePath("storePath", "schemaName", "tableName")));
    }

    @Test public void getSchemaNamesTest() {

        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.LOCAL;
            }

            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                return new LocalCarbonFile("storePath/schemaName");
            }
        };

        new MockUp<LocalCarbonFile>() {
            @Mock
            public CarbonFile[] listFiles() {
                CarbonFile[] carbonFiles = {new LocalCarbonFile("storePath/schemaName")};
                return carbonFiles;
            }
        };

        List<String> schemaNames = carbonTableReader.getSchemaNames();
        assert (schemaNames.get(0).equals("schemaName"));
    }

}
