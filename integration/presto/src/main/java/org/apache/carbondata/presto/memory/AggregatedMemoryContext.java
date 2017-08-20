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
package org.apache.carbondata.presto.memory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This class should remain exactly the same as AggregatedMemoryContext in com.facebook.presto.memory
 **/
public class AggregatedMemoryContext extends AbstractAggregatedMemoryContext {

  private final AbstractAggregatedMemoryContext parentMemoryContext;
  private long usedBytes;
  private boolean closed;

  public AggregatedMemoryContext() {
    this.parentMemoryContext = null;
  }

  public AggregatedMemoryContext(AbstractAggregatedMemoryContext parentMemoryContext) {
    this.parentMemoryContext = requireNonNull(parentMemoryContext, "parentMemoryContext is null");
  }

  @Override protected void updateBytes(long bytes) {
    checkState(!closed);
    if (parentMemoryContext != null) {
      parentMemoryContext.updateBytes(bytes);
    }
    usedBytes += bytes;
  }


  @Override public String toString() {
    return toStringHelper(this).add("usedBytes", usedBytes).add("closed", closed).toString();
  }
}
