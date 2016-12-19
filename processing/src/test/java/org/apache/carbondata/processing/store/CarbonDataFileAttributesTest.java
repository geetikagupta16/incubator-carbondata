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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CarbonDataFileAttributesTest {

  @Test public void testGetFactTimeStamp() {
    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(1, "12-12-2016 00:00:00");
    String actualResult = carbonDataFileAttributes.getFactTimeStamp();
    String expectedResult = "1481481000000";
    assertEquals(expectedResult, actualResult);
  }

  @Test public void testGetFactTimeStampExceptionCase() {
    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(1, "12-12-yyyy 00:00:00");
    String actualResult = carbonDataFileAttributes.getFactTimeStamp();
    assertNull(actualResult);
  }

}
