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
import static org.junit.Assert.assertTrue;

import com.google.genomics.v1.Variant;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the LdVariantInfo class.
 */
@RunWith(JUnit4.class)
public class LdVariantInfoTest {
  private static LdVariantInfo INFO = new LdVariantInfo("chr15", 10, 20, "id", "rsid", 1, "A", "C");

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void constructorArgOrder() {
    assertEquals(INFO.getReferenceName(), "chr15");
    assertEquals(INFO.getStart(), 10);
    assertEquals(INFO.getEnd(), 20);
    assertEquals(INFO.getCloudId(), "id");
    assertEquals(INFO.getRsIds(), "rsid");
    assertEquals(INFO.getAlternateBasesCount(), 1);
    assertEquals(INFO.getZeroAlleleBases(), "A");
    assertEquals(INFO.getOneAlleleBases(), "C");
  }

  @Test
  public void validToString() {
    assertEquals(INFO.toString(), "chr15,10,20,id,rsid,1,A,C");
  }

  @Test
  public void comparison() {
    assertEquals(INFO.compareTo(INFO), 0);
    assertTrue(INFO.equals(INFO));

    LdVariantInfo smallerChromAndPos = new LdVariantInfo("chr11", 5, 6, "id", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(smallerChromAndPos), 1);
    assertEquals(smallerChromAndPos.compareTo(INFO), -1);

    LdVariantInfo smallerChrom = new LdVariantInfo("chr11", 55, 56, "id", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(smallerChrom), 1);
    assertEquals(smallerChrom.compareTo(INFO), -1);

    LdVariantInfo smallerPos = new LdVariantInfo("chr15", 5, 6, "id", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(smallerPos), 1);
    assertEquals(smallerPos.compareTo(INFO), -1);

    LdVariantInfo smallerPos2 = new LdVariantInfo("chr15", 5, 25, "id", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(smallerPos2), 1);
    assertEquals(smallerPos2.compareTo(INFO), -1);

    LdVariantInfo smallerPos3 = new LdVariantInfo("chr15", 10, 15, "id", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(smallerPos3), 1);
    assertEquals(smallerPos3.compareTo(INFO), -1);

    LdVariantInfo smallerId = new LdVariantInfo("chr15", 10, 20, "i", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(smallerId), 1);
    assertEquals(smallerId.compareTo(INFO), -1);

    LdVariantInfo largerId = new LdVariantInfo("chr15", 10, 20, "zzz", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(largerId), -1);
    assertEquals(largerId.compareTo(INFO), 1);

    LdVariantInfo largerEnd = new LdVariantInfo("chr15", 10, 25, "id", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(largerEnd), -1);
    assertEquals(largerEnd.compareTo(INFO), 1);

    LdVariantInfo largerStart = new LdVariantInfo("chr15", 15, 16, "id", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(largerStart), -1);
    assertEquals(largerStart.compareTo(INFO), 1);

    LdVariantInfo largerStart2 = new LdVariantInfo("chr15", 15, 20, "id", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(largerStart2), -1);
    assertEquals(largerStart2.compareTo(INFO), 1);

    LdVariantInfo largerChrom = new LdVariantInfo("chr22", 1, 2, "a", "rsid", 1, "A", "C");
    assertEquals(INFO.compareTo(largerChrom), -1);
    assertEquals(largerChrom.compareTo(INFO), 1);

    LdVariantInfo eq = new LdVariantInfo("chr15", 10, 20, "id", "rsid", 1, "A", "CTGCT");
    assertEquals(INFO.compareTo(eq), 0);
  }

  @Test
  public void fromVariant() {
    Variant var = Variant.newBuilder()
        .setReferenceName("chr15")
        .setStart(10)
        .setEnd(20)
        .setId("id")
        .addNames("rsid")
        .setReferenceBases("A")
        .addAlternateBases("C")
        .build();
    LdVariantInfo varInfo = LdVariantInfo.fromVariant(var, 0, 1);
    assertTrue(INFO.equals(varInfo));
  }

  @Test
  public void noChrom() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing referenceName");
    new LdVariantInfo("", 10, 20, "id", "rsid", 1, "A", "T");
  }

  @Test
  public void invalidStart() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("start < 0: -5");
    new LdVariantInfo("chr1", -5, 20, "id", "rsid", 1, "A", "T");
  }

  @Test
  public void invalidSpan() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("start (3) >= end (3)");
    new LdVariantInfo("chr1", 3, 3, "id", "rsid", 1, "A", "T");
  }

  @Test
  public void noCloudId() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing Cloud ID");
    new LdVariantInfo("chr1", 10, 20, "", "rsid", 1, "A", "T");
  }

  @Test
  public void invalidAltAlleles() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("alternateBasesCount <= 0: 0");
    new LdVariantInfo("chr1", 10, 20, "id", "rsid", 0, "A", "T");
  }

  @Test
  public void noZeroBases() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing zero allele");
    new LdVariantInfo("chr1", 10, 20, "id", "rsid", 1, "", "T");
  }

  @Test
  public void noOneAllele() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing one allele");
    new LdVariantInfo("chr1", 10, 20, "id", "rsid", 1, "A", "");
  }
}
