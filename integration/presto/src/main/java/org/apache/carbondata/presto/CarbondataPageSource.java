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

package org.apache.carbondata.presto;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.result.BatchResult;

import com.facebook.presto.hadoop.$internal.com.google.common.base.Throwables;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Carbondata Page Source class for custom Carbondata RecordSet Iteration.
 */
public class CarbondataPageSource implements ConnectorPageSource {

  private static final int ROWS_PER_REQUEST = 4096;
  private final RecordCursor cursor;
  private final List<Type> types;
  private final PageBuilder pageBuilder;
  private boolean closed;
  private final char[] buffer = new char[100];
  private CarbonIterator<BatchResult> columnCursor;
  private PrestoDictionaryDecodeReadSupport<Object[]> readSupport;
  private Block[] blocks;

  public CarbondataPageSource(RecordSet recordSet) {
    this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(), recordSet.cursor());
  }

  public CarbondataPageSource(List<Type> types, RecordCursor cursor) {
    this.cursor = requireNonNull(cursor, "cursor is null");
    this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
    this.pageBuilder = new PageBuilder(this.types);
    this.columnCursor = ((CarbondataRecordCursor) cursor).getColumnCursor();
    this.readSupport = ((CarbondataRecordCursor) cursor).getReadSupport();
    this.blocks = new Block[types.size()];
  }

  public RecordCursor getCursor() {
    return cursor;
  }

  @Override
  public long getTotalBytes() {
    return cursor.getTotalBytes();
  }

  @Override
  public long getCompletedBytes() {
    return cursor.getCompletedBytes();
  }

  @Override
  public long getReadTimeNanos() {
    return cursor.getReadTimeNanos();
  }

  @Override
  public boolean isFinished() {
    return closed && pageBuilder.isEmpty();
  }

  @Override
  public Page getNextPage() {
    BatchResult columnBatch;
    List<Object[]> columnData;
    blocks = new Block[types.size()];
    int batchSize = 0;
    if (!closed) {
        if (!columnCursor.hasNext()) {
          closed = true;
        } else {
          columnBatch = columnCursor.next();
          columnData = columnBatch.getRows();
          if (types.size() == 0) {
            Object[] data = columnData.get(0);
            pageBuilder.declarePositions(data.length);
          } else {
            pageBuilder.declarePosition();
            for (int column = 0; column < types.size(); column++) {
              BlockBuilder output = pageBuilder.getBlockBuilder(column);
              Object[] data = readSupport.convertColumn(columnData.get(column), column);
              batchSize = data.length;
              blocks[column] = new LazyBlock(data.length, new CarbonBlockLoader(types.get(column), column, data, output));
            }
          }
        }
    }

    // only return a page if the buffer is full or we are finishing
    if (closed) {
      return null;
    }
    Page page;
    if (blocks != null && blocks.length > 0) {
      page = new Page(batchSize, blocks);
    } else {
      page = pageBuilder.build();
    }
    pageBuilder.reset();
    return page;
  }


  @Override
  public long getSystemMemoryUsage() {
    return cursor.getSystemMemoryUsage() + pageBuilder.getSizeInBytes();
  }

  @Override
  public void close() throws IOException {
    // some hive input formats are broken and bad things can happen if you close them multiple times
    if (closed) {
      return;
    }
    closed = true;

    try {
      columnCursor.close();
      cursor.close();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }


  }

  private long parseLong(DecimalType type, Slice slice, int offset, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, offset, length);
    return decimal.unscaledValue().longValue();
  }


  private Slice parseSlice(DecimalType type, Slice slice, int offset, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, offset, length);
    return encodeUnscaledValue(decimal.unscaledValue());
  }

  private BigDecimal parseBigDecimal(DecimalType type, Slice slice, int offset, int length) {
    checkArgument(length < buffer.length);
    for (int i = 0; i < length; i++) {
      buffer[i] = (char) slice.getByte(offset + i);
    }
    BigDecimal decimal = new BigDecimal(buffer, 0, length);
    checkState(decimal.scale() <= type.getScale(), "Read decimal value scale larger than column scale");
    decimal = decimal.setScale(type.getScale(), HALF_UP);
    checkState(decimal.precision() <= type.getPrecision(), "Read decimal precision larger than column precision");
    return decimal;
  }

  public boolean getBoolean(Object value) {
    return (Boolean) value;
  }

  public long getLong(Object obj, Type actual) {
    Long timeStr = 0L;
    if (obj instanceof Integer) {
      timeStr = ((Integer) obj).longValue();
    } else if (obj instanceof Long) {
      timeStr = (Long) obj;
    } else {
      timeStr = Math.round(Double.parseDouble(obj.toString()));
    }
    if (actual instanceof TimestampType) {
      return new Timestamp(timeStr).getTime() / 1000;
    }
    //suppose the
    return timeStr;
  }

  public double getDouble(Object value) {
    return (Double) value;
  }

  public Slice getSlice(Object value, Type type) {
    Type decimalType = type;
    if (decimalType instanceof DecimalType) {
      DecimalType actual = (DecimalType) decimalType;
     /* CarbondataColumnHandle carbondataColumnHandle = columnHandles.get(field);
      if(carbondataColumnHandle.getPrecision() > 0 ) {
        checkFieldType(field, DecimalType.createDecimalType(carbondataColumnHandle.getPrecision(), carbondataColumnHandle.getScale()));
      } else {
        checkFieldType(field, DecimalType.createDecimalType());
      }*/
      Object fieldValue = value;
      BigDecimal bigDecimalValue = new BigDecimal(fieldValue.toString());
      if (isShortDecimal(decimalType)) {
        return utf8Slice(Decimals.toString(bigDecimalValue.longValue(), actual.getScale()));
      } else {
        if (bigDecimalValue.scale() > actual.getScale()) {
          BigInteger unscaledDecimal =
                  rescale(bigDecimalValue.unscaledValue(), bigDecimalValue.scale(),
                          bigDecimalValue.scale());
          Slice decimalSlice = Decimals.encodeUnscaledValue(unscaledDecimal);
          return utf8Slice(Decimals.toString(decimalSlice, actual.getScale()));
          //return decimalSlice;
        } else {
          BigInteger unscaledDecimal =
                  rescale(bigDecimalValue.unscaledValue(), bigDecimalValue.scale(), actual.getScale());
          Slice decimalSlice = Decimals.encodeUnscaledValue(unscaledDecimal);
          return utf8Slice(Decimals.toString(decimalSlice, actual.getScale()));
          //return decimalSlice;

        }

      }
    } else {
      return utf8Slice(value.toString());
    }
  }

  public Object getObject(Object value) {
    return value;
  }

  public boolean isNull(Object value) {

    return value == null;
  }

  /**
   * Using the LazyBlockLoader
   */
  private final class CarbonBlockLoader implements LazyBlockLoader<LazyBlock> {
    private boolean loaded;
    private Type type;
    int colIndex;
    Object[] data;
    BlockBuilder output;

    public CarbonBlockLoader(Type type, int colIndex, Object[] data, BlockBuilder output) {
      this.type = type;
      this.colIndex = colIndex;
      this.data = data;
      this.output = output;
    }

    @Override
    public void load(LazyBlock block) {
      if (loaded) {
        return;
      }
      Class<?> javaType = type.getJavaType();
      if (data != null) {
        for (Object value : data) {
          /*if(column == 0) {
            pageBuilder.declarePosition();
          }*/
          if (value == null) {
            output.appendNull();
          } else {
            if (javaType == boolean.class) {
              type.writeBoolean(output, getBoolean(value));
            } else if (javaType == long.class) {
              type.writeLong(output, getLong(value, type));
            } else if (javaType == double.class) {
              type.writeDouble(output, getDouble(value));
            } else if (javaType == Slice.class) {
              Slice slice = getSlice(value, type);
              if (type instanceof DecimalType) {
                if (isShortDecimal(type)) {
                  type.writeLong(output, parseLong((DecimalType) type, slice, 0, slice.length()));
                } else {
                  type.writeSlice(output, parseSlice((DecimalType) type, slice, 0, slice.length()));
                }
              } else {
                type.writeSlice(output, slice, 0, slice.length());
              }
            } else {
              type.writeObject(output, cursor.getObject(colIndex));
            }
          }
        }
        block.setBlock(output.build());
        loaded = true;
      }
    }
  }
}
