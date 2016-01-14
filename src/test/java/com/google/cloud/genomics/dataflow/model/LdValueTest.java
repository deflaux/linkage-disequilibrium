/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dataflow.model;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the LdValue class.
 */
@RunWith(JUnit4.class)
public class LdValueTest {
  private static LdVariantInfo QUERY = new LdVariantInfo("chr2", 1, 2, "i1", "rsid", 1, "A", "C");
  private static LdVariantInfo TARGET = new LdVariantInfo("chr2", 8, 9, "i2", "", 1, "G", "CTTG");
  private static LdValue EXPECTED = new LdValue(QUERY, TARGET, 100, 75, 50, 25, 0.5, 1.0);

  @Test
  public void constructorArgOrder() {
    assertEquals(EXPECTED.getQuery(), QUERY);
    assertEquals(EXPECTED.getTarget(), TARGET);
    assertEquals(EXPECTED.getCompCount(), 100);
    assertEquals(EXPECTED.getQueryOneAlleleCount(), 75);
    assertEquals(EXPECTED.getTargetOneAlleleCount(), 50);
    assertEquals(EXPECTED.getQueryAndTargetOneAlleleCount(), 25);
    assertEquals(EXPECTED.getR(), .5, 1e-9);
    assertEquals(EXPECTED.getDPrime(), 1.0, 1e-9);
  }

  @Test
  public void toStringAndBack() {
    assertEquals(
        EXPECTED.toString(),
        "chr2,1,2,i1,rsid,1,A,C,chr2,8,9,i2,,1,G,CTTG,100,75,50,25,0.500000,1.000000");
    assertEquals(EXPECTED.toString(), LdValue.fromLine(EXPECTED.toString()).toString());
  }

  @Test
  public void reverseWorks() {
    assertEquals(
        EXPECTED.reverse().toString(),
        "chr2,8,9,i2,,1,G,CTTG,chr2,1,2,i1,rsid,1,A,C,100,50,75,25,0.500000,1.000000");
  }
}
