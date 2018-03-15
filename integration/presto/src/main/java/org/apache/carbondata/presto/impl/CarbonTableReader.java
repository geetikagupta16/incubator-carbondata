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

package org.apache.carbondata.presto.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.presto.PrestoFilterUtil;

import com.facebook.presto.hadoop.$internal.com.google.gson.Gson;
import com.facebook.presto.hadoop.$internal.io.netty.util.internal.ConcurrentSet;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.thrift.TBase;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * CarbonTableReader will be a facade of these utils
 * 1:CarbonMetadata,(logic table)
 * 2:FileFactory, (physic table file)
 * 3:CarbonCommonFactory, (offer some )
 * 4:DictionaryFactory, (parse dictionary util)
 * Currently, it is mainly used to parse metadata of tables under
 * the configured carbondata-store path and filter the relevant
 * input splits with given query predicates.
 */
public class CarbonTableReader {

  // default PathFilter, accepts files in carbondata format (with .carbondata extension).
  private static final PathFilter DefaultFilter = new PathFilter() {
    @Override public boolean accept(Path path) {
      return CarbonTablePath.isCarbonDataFile(path.getName());
    }
  };
  private CarbonTableConfig config;
  /**
   * The names of the tables under the schema (this.carbonFileList).
   */
  private ConcurrentSet<SchemaTableName> tableList;
  /**
   * carbonFileList represents the store path of the schema, which is configured as carbondata-store
   * in the CarbonData catalog file ($PRESTO_HOME$/etc/catalog/carbondata.properties).
   */
  private CarbonFile carbonFileList;
  private FileFactory.FileType fileType;
  /**
   * A cache for Carbon reader, with this cache,
   * metadata of a table is only read from file system once.
   */
  private AtomicReference<HashMap<SchemaTableName, CarbonTableCacheModel>> carbonCache;

  private CarbonTableInputFormat carbonTableInputFormat;

  private  LoadMetadataDetails[] loadMetadataDetails;

  private  PartitionInfo partitionInfo;

  @Inject public CarbonTableReader(CarbonTableConfig config) {
    this.config = requireNonNull(config, "CarbonTableConfig is null");
    this.carbonCache = new AtomicReference(new HashMap());
    tableList = new ConcurrentSet<>();
  }

  /**
   * For presto worker node to initialize the metadata cache of a table.
   *
   * @param table the name of the table and schema.
   * @return
   */
  public CarbonTableCacheModel getCarbonCache(SchemaTableName table) {

    if (!carbonCache.get().containsKey(table) || carbonCache.get().get(table) == null) {
      // if this table is not cached, try to read the metadata of the table and cache it.
      try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
          FileFactory.class.getClassLoader())) {
        if (carbonFileList == null) {
          fileType = FileFactory.getFileType(config.getStorePath());
          try {
            carbonFileList = FileFactory.getCarbonFile(config.getStorePath(), fileType);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      updateSchemaTables(table);
      parseCarbonMetadata(table);
    }
    if (carbonCache.get().containsKey(table)) {
      return carbonCache.get().get(table);
    } else {
      return null;
    }
  }

  private void removeTableFromCache(SchemaTableName table) {
    DataMapStoreManager.getInstance()
        .clearDataMaps(carbonCache.get().get(table).carbonTable.getAbsoluteTableIdentifier());
    carbonCache.get().remove(table);
    tableList.remove(table);

  }

  /**
   * Return the schema names under a schema store path (this.carbonFileList).
   *
   * @return
   */
  public List<String> getSchemaNames() {
    return updateSchemaList();
  }

  /**
   * Get the CarbonFile instance which represents the store path in the configuration, and assign it to
   * this.carbonFileList.
   *
   * @return
   */
  private boolean updateCarbonFile() {
    if (carbonFileList == null) {
      fileType = FileFactory.getFileType(config.getStorePath());
      try {
        carbonFileList = FileFactory.getCarbonFile(config.getStorePath(), fileType);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return true;
  }

  /**
   * Return the schema names under a schema store path (this.carbonFileList).
   *
   * @return
   */
  private List<String> updateSchemaList() {
    updateCarbonFile();

    if (carbonFileList != null) {
      return Stream.of(carbonFileList.listFiles()).map(CarbonFile::getName).collect(Collectors.toList());
    } else return ImmutableList.of();
  }

  /**
   * Get the names of the tables in the given schema.
   *
   * @param schema name of the schema
   * @return
   */
  public Set<String> getTableNames(String schema) {
    requireNonNull(schema, "schema is null");
    return updateTableList(schema);
  }

  /**
   * Get the names of the tables in the given schema.
   *
   * @param schemaName name of the schema
   * @return
   */
  private Set<String> updateTableList(String schemaName) {
    List<CarbonFile> schema =
        Stream.of(carbonFileList.listFiles()).filter(a -> schemaName.equals(a.getName()))
            .collect(Collectors.toList());
    if (schema.size() > 0) {
      return Stream.of((schema.get(0)).listFiles()).map(CarbonFile::getName)
          .collect(Collectors.toSet());
    } else return ImmutableSet.of();
  }

  /**
   * Get the CarbonTable instance of the given table.
   *
   * @param schemaTableName name of the given table.
   * @return
   */
  public CarbonTable getTable(SchemaTableName schemaTableName) {
    try {
      updateSchemaTables(schemaTableName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    requireNonNull(schemaTableName, "schemaTableName is null");
    return loadTableMetadata(schemaTableName);
  }

  /**
   * Find all the tables under the schema store path (this.carbonFileList)
   * and cache all the table names in this.tableList. Notice that whenever this method
   * is called, it clears this.tableList and populate the list by reading the files.
   */
  private void updateSchemaTables(SchemaTableName schemaTableName) {
    // update logic determine later
    boolean isKeyExists = carbonCache.get().containsKey(schemaTableName);

    if (carbonFileList == null) {
      updateSchemaList();
    }
    try {
      if (isKeyExists
          && !FileFactory.isFileExist(
          CarbonTablePath.getSchemaFilePath(
              carbonCache.get().get(schemaTableName).carbonTable.getTablePath()), fileType)) {
        removeTableFromCache(schemaTableName);
        throw new TableNotFoundException(schemaTableName);
      }
    } catch (IOException e) {
      throw new RuntimeException();
    }

    if (isKeyExists) {
      CarbonTableCacheModel ctcm = carbonCache.get().get(schemaTableName);
      if(ctcm != null && ctcm.carbonTable.getTableInfo() != null) {
        Long latestTime = FileFactory.getCarbonFile(
            CarbonTablePath.getSchemaFilePath(
                carbonCache.get().get(schemaTableName).carbonTable.getTablePath())
        ).getLastModifiedTime();
        Long oldTime = ctcm.carbonTable.getTableInfo().getLastUpdatedTime();
        if (DateUtils.truncate(new Date(latestTime), Calendar.MINUTE)
            .after(DateUtils.truncate(new Date(oldTime), Calendar.MINUTE))) {
          removeTableFromCache(schemaTableName);
        }
      }
    }
    if (!tableList.contains(schemaTableName)) {
      for (CarbonFile cf : carbonFileList.listFiles()) {
        if (!cf.getName().endsWith(".mdt")) {
          for (CarbonFile table : cf.listFiles()) {
            tableList.add(new SchemaTableName(cf.getName(), table.getName()));
          }
        }
      }
    }
  }


  /**
   * Find the table with the given name and build a CarbonTable instance for it.
   * This method should be called after this.updateSchemaTables().
   *
   * @param schemaTableName name of the given table.
   * @return
   */
  private CarbonTable loadTableMetadata(SchemaTableName schemaTableName) {
    for (SchemaTableName table : tableList) {
      if (!table.equals(schemaTableName)) continue;

      return parseCarbonMetadata(table);
    }
    throw new TableNotFoundException(schemaTableName);
  }

  /**
   * Read the metadata of the given table and cache it in this.carbonCache (CarbonTableReader cache).
   *
   * @param table name of the given table.
   * @return the CarbonTable instance which contains all the needed metadata for a table.
   */
  private CarbonTable parseCarbonMetadata(SchemaTableName table) {
    CarbonTable result = null;
    try {
      CarbonTableCacheModel cache = carbonCache.get().get(table);
      if (cache == null) {
        cache = new CarbonTableCacheModel();
      }
      if (cache.isValid()) return cache.carbonTable;

      // If table is not previously cached, then:

      // Step 1: get store path of the table and cache it.
      // create table identifier. the table id is randomly generated.
      CarbonTableIdentifier carbonTableIdentifier =
          new CarbonTableIdentifier(table.getSchemaName(), table.getTableName(),
              UUID.randomUUID().toString());
      String storePath = config.getStorePath();
      String tablePath = storePath + "/" + carbonTableIdentifier.getDatabaseName() + "/"
          + carbonTableIdentifier.getTableName();

      //Step 2: read the metadata (tableInfo) of the table.
      ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
        // TBase is used to read and write thrift objects.
        // TableInfo is a kind of TBase used to read and write table information.
        // TableInfo is generated by thrift, see schema.thrift under format/src/main/thrift for details.
        public TBase create() {
          return new org.apache.carbondata.format.TableInfo();
        }
      };
      ThriftReader thriftReader =
          new ThriftReader(CarbonTablePath.getSchemaFilePath(tablePath), createTBase);
      thriftReader.open();
      org.apache.carbondata.format.TableInfo tableInfo =
          (org.apache.carbondata.format.TableInfo) thriftReader.read();
      thriftReader.close();


      // Step 3: convert format level TableInfo to code level TableInfo
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      // wrapperTableInfo is the code level information of a table in carbondata core, different from the Thrift TableInfo.
      TableInfo wrapperTableInfo = schemaConverter
          .fromExternalToWrapperTableInfo(tableInfo, table.getSchemaName(), table.getTableName(),
              tablePath);

      // Step 4: Load metadata info into CarbonMetadata
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);

      cache.carbonTable = CarbonMetadata.getInstance().getCarbonTable(
          table.getSchemaName(), table.getTableName());

      // cache the table
      carbonCache.get().put(table, cache);

      result = cache.carbonTable;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    return result;
  }


  public List<CarbonLocalInputSplit> getInputSplits2(CarbonTableCacheModel tableCacheModel,
      Expression filters, TupleDomain<ColumnHandle> constraints)  {
    List<CarbonLocalInputSplit> result = new ArrayList<>();

    CarbonTable carbonTable = tableCacheModel.carbonTable;
    TableInfo tableInfo = tableCacheModel.carbonTable.getTableInfo();
    Configuration config = new Configuration();
    config.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
    String carbonTablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();
    config.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
    config.set(CarbonTableInputFormat.DATABASE_NAME, carbonTable.getDatabaseName());
    config.set(CarbonTableInputFormat.TABLE_NAME, carbonTable.getTableName());

    try {
      CarbonTableInputFormat.setTableInfo(config, tableInfo);
      carbonTableInputFormat =
          createInputFormat(config, carbonTable.getAbsoluteTableIdentifier(), filters);
      JobConf jobConf = new JobConf(config);

      loadMetadataDetails = SegmentStatusManager
          .readTableStatusFile(CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()));

      partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName());

      if(partitionInfo!=null && partitionInfo.getPartitionType()==PartitionType.NATIVE_HIVE) {
        Set<PartitionSpec> partitionSpecs = new HashSet<>();

        for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
          SegmentFileStore segmentFileStore =
              new SegmentFileStore(carbonTable.getTablePath(), loadMetadataDetail.getSegmentFile());
          partitionSpecs.addAll(segmentFileStore.getPartitionSpecs());
        }

        List<String> partitionValuesFromExpression =
            PrestoFilterUtil.getPartitionFilters(carbonTable, constraints);

        List<String> partitionsNames =
            partitionSpecs.stream().map(PartitionSpec::getPartitions).collect(toList()).stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<PartitionSpec> partitionSpecsList = new ArrayList(partitionSpecs);

        List<PartitionSpec> filteredPartitions = new ArrayList();

        for (String partitionValue : partitionValuesFromExpression) {
          int index = partitionsNames.indexOf(partitionValue);
          if (index != -1) {
            filteredPartitions.add(partitionSpecsList.get(index));

          }
        }
        CarbonTableInputFormat.setPartitionsToPrune(jobConf, new ArrayList<>(filteredPartitions));

      }

      Job job = Job.getInstance(jobConf);


      List<InputSplit> splits = getSplits(job);

      CarbonInputSplit carbonInputSplit ;
      Gson gson = new Gson();
      if (splits != null && splits.size() > 0) {
        for (InputSplit inputSplit : splits) {
          carbonInputSplit = (CarbonInputSplit) inputSplit;
          result.add(new CarbonLocalInputSplit(carbonInputSplit.getSegmentId(),
              carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
              carbonInputSplit.getLength(), Arrays.asList(carbonInputSplit.getLocations()),
              carbonInputSplit.getNumberOfBlocklets(), carbonInputSplit.getVersion().number(),
              carbonInputSplit.getDeleteDeltaFiles(),
              gson.toJson(carbonInputSplit.getDetailInfo())));
        }
      }

    } catch (IOException e) {
      throw new RuntimeException("Error creating Splits from CarbonTableInputFormat", e);
    }

    return result;
  }
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    AbsoluteTableIdentifier identifier = carbonTableInputFormat.getAbsoluteTableIdentifier(job.getConfiguration());

    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(identifier, loadMetadataDetails);
    CarbonTable carbonTable = carbonTableInputFormat.getOrCreateCarbonTable(job.getConfiguration());
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    List<Segment> invalidSegments = new ArrayList<>();
    List<UpdateVO> invalidTimestampsList = new ArrayList<>();
    List<Segment> streamSegments = null;
    // get all valid segments and set them into the configuration
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);
    SegmentStatusManager.ValidAndInvalidSegmentsInfo segments =
        segmentStatusManager.getValidAndInvalidSegments(loadMetadataDetails);

    if (CarbonTableInputFormat.getValidateSegmentsToAccess(job.getConfiguration())) {
      List<Segment> validSegments = segments.getValidSegments();
      streamSegments = segments.getStreamSegments();
      if (validSegments.size() == 0) {
        return carbonTableInputFormat.getSplitsOfStreaming(job, identifier, streamSegments);
      }



      List<Segment> filteredSegmentToAccess = carbonTableInputFormat.getFilteredSegment(job, segments.getValidSegments());
      if (filteredSegmentToAccess.size() == 0) {
        return new ArrayList<>(0);
      } else {
        CarbonTableInputFormat.setSegmentsToAccess(job.getConfiguration(), filteredSegmentToAccess);
      }
      // remove entry in the segment index if there are invalid segments
      invalidSegments.addAll(segments.getInvalidSegments());
      for (Segment invalidSegmentId : invalidSegments) {
        invalidTimestampsList
            .add(updateStatusManager.getInvalidTimestampRange(invalidSegmentId.getSegmentNo()));
      }
      if (invalidSegments.size() > 0) {
        DataMapStoreManager.getInstance()
            .clearInvalidSegments(carbonTableInputFormat.getOrCreateCarbonTable(job.getConfiguration()), invalidSegments);
      }
    }
    ArrayList<Segment> validAndInProgressSegments = new ArrayList<>(segments.getValidSegments());
    // Add in progress segments also to filter it as in case of aggregate table load it loads
    // data from in progress table.
    validAndInProgressSegments.addAll(segments.getListOfInProgressSegments());
    // get updated filtered list
    List<Segment> filteredSegmentToAccess =
        carbonTableInputFormat.getFilteredSegment(job, new ArrayList<>(validAndInProgressSegments));
    // Clean the updated segments from memory if the update happens on segments
    List<Segment> toBeCleanedSegments = new ArrayList<>();
    for (SegmentUpdateDetails segmentUpdateDetail : updateStatusManager
        .getUpdateStatusDetails()) {
      boolean refreshNeeded =
          DataMapStoreManager.getInstance().getTableSegmentRefresher(identifier)
              .isRefreshNeeded(segmentUpdateDetail.getSegmentName(), updateStatusManager);
      if (refreshNeeded) {
        toBeCleanedSegments.add(new Segment(segmentUpdateDetail.getSegmentName(), null));
      }
    }
    // Clean segments if refresh is needed
    for (Segment segment : filteredSegmentToAccess) {
      if (DataMapStoreManager.getInstance().getTableSegmentRefresher(identifier)
          .isRefreshNeeded(segment.getSegmentNo())) {
        toBeCleanedSegments.add(segment);
      }
    }
    if (toBeCleanedSegments.size() > 0) {
      DataMapStoreManager.getInstance()
          .clearInvalidSegments(carbonTableInputFormat.getOrCreateCarbonTable(job.getConfiguration()),
              toBeCleanedSegments);
    }

    // process and resolve the expression
    Expression filter = carbonTableInputFormat.getFilterPredicates(job.getConfiguration());
    TableProvider tableProvider = new SingleTableProvider(carbonTable);
    // this will be null in case of corrupt schema file.
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable, null, null);

    // prune partitions for filter query on partition table
    BitSet matchedPartitions = null;
    if (partitionInfo != null && partitionInfo.getPartitionType() != PartitionType.NATIVE_HIVE) {
      matchedPartitions = carbonTableInputFormat.setMatchedPartitions(null, filter, partitionInfo, null);
      if (matchedPartitions != null) {
        if (matchedPartitions.cardinality() == 0) {
          return new ArrayList<InputSplit>();
        } else if (matchedPartitions.cardinality() == partitionInfo.getNumPartitions()) {
          matchedPartitions = null;
        }
      }
    }

    FilterResolverIntf filterInterface = CarbonInputFormatUtil
        .resolveFilter(filter, carbonTable.getAbsoluteTableIdentifier(), tableProvider);

    // do block filtering and get split
    List<InputSplit> splits =
        carbonTableInputFormat.getSplits(job, filterInterface, filteredSegmentToAccess, matchedPartitions, partitionInfo,
            null, updateStatusManager);
    // pass the invalid segment to task side in order to remove index entry in task side
    if (invalidSegments.size() > 0) {
      for (InputSplit split : splits) {
        ((org.apache.carbondata.hadoop.CarbonInputSplit) split).setInvalidSegments(invalidSegments);
        ((org.apache.carbondata.hadoop.CarbonInputSplit) split)
            .setInvalidTimestampRange(invalidTimestampsList);
      }
    }

    // add all splits of streaming
    List<InputSplit> splitsOfStreaming = carbonTableInputFormat.getSplitsOfStreaming(job, identifier, streamSegments);
    if (!splitsOfStreaming.isEmpty()) {
      splits.addAll(splitsOfStreaming);
    }
    return splits;
  }

  private CarbonTableInputFormat<Object>  createInputFormat( Configuration conf,
      AbsoluteTableIdentifier identifier, Expression filterExpression)
      throws IOException {
    CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
    CarbonTableInputFormat.setTablePath(conf,
        identifier.appendWithLocalPrefix(identifier.getTablePath()));
    CarbonTableInputFormat.setFilterPredicates(conf, filterExpression);

    return format;
  }


}