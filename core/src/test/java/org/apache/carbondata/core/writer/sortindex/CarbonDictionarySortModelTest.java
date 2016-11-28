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
package org.apache.carbondata.core.writer.sortindex;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CarbonDictionarySortModelTest {

  private CarbonDictionarySortModel carbonDictionarySortModel;

  @Test public void testCompareToForDataTypeDoubleCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DOUBLE, "7234");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DOUBLE, "5678");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == 1);
  }

  @Test public void testCompareToForDataTypeDoubleExceptionCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DOUBLE, "double");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DOUBLE, "@NU#LL$!");
    System.out.println(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel));
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == -1);
  }

  @Test public void testCompareToForDataTypeDoubleExceptionCaseForOtherObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DOUBLE, "1234");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DOUBLE, "@NU#LL$!");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == -1);
  }

  @Test public void testCompareToForDataTypeBooleanCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.BOOLEAN, "memberValue");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DOUBLE, "value");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == -9);
  }

  @Test public void testCompareToForDataTypeDecimalCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "72.34");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DECIMAL, "56.78");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == 1);
  }

  @Test public void testCompareToForDataTypeDecimalExceptionCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "decimal");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DECIMAL, "@NU#LL$!");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == -1);
  }

  @Test public void testCompareToForDataTypeDecimalExceptionCaseForOtherObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "15.24");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DECIMAL, "@NU#LL$!");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == -1);
  }

  @Test public void testCompareToForDataTypeTimestampCase() {
    carbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataType.TIMESTAMP, "2014-09-22 12:00:00");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.TIMESTAMP, "2015-09-22 12:08:49");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == -1);
  }

  @Test public void testCompareToForDataTypeTimestampExceptionCase() {
    carbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataType.TIMESTAMP, "2014-09 12:00:00");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.TIMESTAMP, "@NU#LL$!");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == -1);
  }

  @Test public void testCompareToForDataTypeTimestampExceptionCaseForOtherObject() {
    carbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataType.TIMESTAMP, "2014-09-22 12:00:00");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.TIMESTAMP, "2014-09-22 12");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == -1);
  }

  @Test public void testHashCode() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "15.24");
    assertTrue(carbonDictionarySortModel.hashCode() == 46877260);

  }

  @Test public void testHashCodeNullCaseForMemberValue() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, null);
    assert (carbonDictionarySortModel.hashCode() == 0);
  }

  @Test public void testEquals() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "15.24");
    CarbonDictionarySortModel testCarbonDictionarySortModel = carbonDictionarySortModel;
    assertTrue(carbonDictionarySortModel.equals(testCarbonDictionarySortModel));
  }

  @Test public void testEqualsMemberValueNullCase() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, null);
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataType.BOOLEAN, "false");
    assertFalse(carbonDictionarySortModel.equals(testCarbonDictionarySortModel));
  }

  @Test public void testEqualsWhenMemberValueDiffers() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "12.45");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataType.BOOLEAN, "false");
    assertFalse(carbonDictionarySortModel.equals(testCarbonDictionarySortModel));
  }

  @Test public void testEqualsWhenMemberValueIsSame() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "12.45");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(1, DataType.DECIMAL, "12.45");
    assertTrue(carbonDictionarySortModel.equals(testCarbonDictionarySortModel));
  }

  @Test public void testEqualsForDifferentObjects() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "12.45");
    Object testCarbonDictionarySortModel = new Object();
    assertFalse(carbonDictionarySortModel.equals(testCarbonDictionarySortModel));
  }

  @Test public void testCompareToForDataTypeDoubleExceptionCaseForDifferentObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DOUBLE, "double");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DOUBLE, "1234");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == 1);
  }

  @Test public void testCompareToForDataTypeDecimalExceptionCaseForDifferentObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.DECIMAL, "12.il");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.DECIMAL, "12.89");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == 1);
  }

  @Test public void testCompareToForDataTypeTimestampExceptionCaseForDifferentObject() {
    carbonDictionarySortModel = new CarbonDictionarySortModel(1, DataType.TIMESTAMP, "2014-09");
    CarbonDictionarySortModel testCarbonDictionarySortModel =
        new CarbonDictionarySortModel(2, DataType.TIMESTAMP, "2014-09-22 12:00:00");
    assertTrue(carbonDictionarySortModel.compareTo(testCarbonDictionarySortModel) == 1);
  }

}
