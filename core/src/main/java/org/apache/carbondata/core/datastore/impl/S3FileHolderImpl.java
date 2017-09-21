package org.apache.carbondata.core.datastore.impl;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileHolder;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class S3FileHolderImpl implements FileHolder {

  private Map<String, FSDataInputStream> fileNameAndStreamCache;

  private String queryId;

  public S3FileHolderImpl() {
    this.fileNameAndStreamCache =
        new HashMap<String, FSDataInputStream>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }


  @Override public ByteBuffer readByteBuffer(String filePath, long offset, int length)
      throws IOException {
    byte[] readByteArray = readByteArray(filePath, offset, length);
    ByteBuffer byteBuffer = ByteBuffer.wrap(readByteArray);
    byteBuffer.rewind();
    return byteBuffer;
  }


  /**
   * This method will be used to check whether stream is already present in
   * cache or not for filepath if not present then create it and then add to
   * cache, other wise get from cache
   *
   * @param filePath fully qualified file path
   * @return channel
   */
  private FSDataInputStream updateCache(String filePath) throws IOException {
    FSDataInputStream fileChannel = fileNameAndStreamCache.get(filePath);
    if (null == fileChannel) {
      Path pt = new Path(filePath);
      FileSystem fs = new CarbonS3FileSystem();
      fs.initialize(pt.toUri(), FileFactory.getConfiguration());
      fileChannel = fs.open(pt);
      fileNameAndStreamCache.put(filePath, fileChannel);
    }
    return fileChannel;
  }

  /**
   * This method will be used to read from file based on number of bytes to be read and position
   *
   * @param channel file channel
   * @param size    number of bytes
   * @param offset  position
   * @return byte buffer
   */
  private byte[] read(FSDataInputStream channel, int size, long offset) throws IOException {
    byte[] byteBffer = new byte[size];
    channel.seek(offset);
    channel.readFully(byteBffer);
    return byteBffer;
  }

  /**
   * This method will be used to read from file based on number of bytes to be read and position
   *
   * @param channel file channel
   * @param size    number of bytes
   * @return byte buffer
   */
  private byte[] read(FSDataInputStream channel, int size) throws IOException {
    byte[] byteBffer = new byte[size];
    channel.readFully(byteBffer);
    return byteBffer;
  }


  @Override public byte[] readByteArray(String filePath, long offset, int length)
      throws IOException {
    FSDataInputStream fileChannel = updateCache(filePath);
    return read(fileChannel, length, offset);
  }

  @Override public byte[] readByteArray(String filePath, int length) throws IOException {
    FSDataInputStream fileChannel = updateCache(filePath);
    return read(fileChannel, length);
  }

  @Override public int readInt(String filePath, long offset) throws IOException {
    FSDataInputStream fileChannel = updateCache(filePath);
    fileChannel.seek(offset);
    return fileChannel.readInt();
  }

  @Override public long readLong(String filePath, long offset) throws IOException {
    FSDataInputStream fileChannel = updateCache(filePath);
    fileChannel.seek(offset);
    return fileChannel.readLong();
  }

  @Override public int readInt(String filePath) throws IOException {
    FSDataInputStream fileChannel = updateCache(filePath);
    return fileChannel.readInt();
  }

  @Override public long readDouble(String filePath, long offset) throws IOException {
    FSDataInputStream fileChannel = updateCache(filePath);
    fileChannel.seek(offset);
    return fileChannel.readLong();
  }

  @Override public void finish() throws IOException {
    for (Map.Entry<String, FSDataInputStream> entry : fileNameAndStreamCache.entrySet()) {
      FSDataInputStream channel = entry.getValue();
      if (null != channel) {
        channel.close();
      }
    }

  }

  @Override public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  @Override public String getQueryId() {
    return queryId;
  }

  @Override public DataInputStream getDataInputStream(String filePath, long offset)
      throws IOException {
    FSDataInputStream fsDataInputStream = updateCache(filePath);
    fsDataInputStream.seek(offset);
    return new DataInputStream(new BufferedInputStream(fsDataInputStream, 1 * 1024 * 1024));
  }
}