package org.apache.carbondata.hadoop

import org.apache.carbondata.core.cache.CacheType

import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.cache.{Cache, CacheProvider}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn

class DictionaryDecodeReaderSupport2[T] {

  def initialize(carbonColumns: Array[CarbonColumn],
      absoluteTableIdentifier: AbsoluteTableIdentifier): Array[(DataType, Dictionary, Int)] = {

    carbonColumns.zipWithIndex.filter(dictChecker(_)).map {carbonColumnWithIndex =>
      val (carbonColumn, index) = carbonColumnWithIndex
          val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
            CacheProvider.getInstance().createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier
            .getStorePath)
          val dict: Dictionary =forwardDictionaryCache.get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier, carbonColumn.getColumnIdentifier, carbonColumn.getDataType))
          (carbonColumn.getDataType,dict, index)
    }
  }

  def readRow(data: Array[Object], dictionaries: Array[(DataType, Dictionary, Int)]): Array[Object] = {
    dictionaries.foreach { (dictionary: (DataType, Dictionary, Int)) =>
      val (_, dict, position) = dictionary
      data(position) = dict.getDictionaryValueForKey(data(position).asInstanceOf[Int])
    }
    data
  }

  private def dictChecker(carbonColumWithIndex: (CarbonColumn, Int)): Boolean = {
    val (carbonColumn, _) = carbonColumWithIndex
    if (!carbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY) && !carbonColumn.isComplex &&
        carbonColumn.hasEncoding(Encoding.DICTIONARY)) {
      true
    }else{
      false
    }
  }
}
