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
package org.apache.carbondata.presto.scan.result;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.presto.scan.result.iterator.AbstractDetailQueryResultIterator;

/**
 * It reads the data in column batch format
 */
public class ColumnBasedResultIterator extends AbstractDetailQueryResultIterator<Object> {

  private final Object lock = new Object();

  public ColumnBasedResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel,
      ExecutorService execService) {
    super(infos, queryModel, execService);
  }

  @Override public BatchResult next() {
    return getBatchResult();
  }

  private BatchResult getBatchResult() {
    BatchResult batchResult = new BatchResult();
    synchronized (lock) {
      updateDataBlockIterator();
      if (dataBlockIterator != null) {
        batchResult.setRows(dataBlockIterator.processNextColumnBatch());
      }
    }
    return batchResult;
  }
}