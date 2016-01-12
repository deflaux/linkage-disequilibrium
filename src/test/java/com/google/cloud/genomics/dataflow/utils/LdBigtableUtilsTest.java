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
package com.google.cloud.genomics.dataflow.utils;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the LdBigtableUtils class.
 */
@RunWith(JUnit4.class)
public class LdBigtableUtilsTest {

  // The length of the BigTable key in bytes.
  private static final int KEY_LENGTH = 16;

  @Test
  public void chromosomeHashHasNoCollisions() {
    List<String> chromosomes = Arrays.asList(
        "1", "chr1",
        "2", "chr2",
        "3", "chr3",
        "4", "chr4",
        "5", "chr5",
        "6", "chr6",
        "7", "chr7",
        "8", "chr8",
        "9", "chr9",
        "10", "chr10",
        "11", "chr11",
        "12", "chr12",
        "13", "chr13",
        "14", "chr14",
        "15", "chr15",
        "16", "chr16",
        "17", "chr17",
        "18", "chr18",
        "19", "chr19",
        "20", "chr20",
        "21", "chr21",
        "22", "chr22",
        "X", "chrX",
        "Y", "chrY",
        "M", "MT", "chrM", "chrMT");

    Set<String> keySet = new HashSet<>();
    for (String chromosome : chromosomes) {
      keySet.add(Arrays.toString(LdBigtableUtils.keyStart(chromosome)));
    }
    assertEquals(keySet.size(), chromosomes.size());
  }

  @Test
  public void hashOrdersCorrectly() {
    assertEquals(0, compareKeys(LdBigtableUtils.keyStart("1"), LdBigtableUtils.keyStart("1", 0)));
    assertEquals(-1, compareKeys(LdBigtableUtils.keyStart("1"), LdBigtableUtils.keyStart("1", 1)));
    assertEquals(1,
        compareKeys(LdBigtableUtils.keyStart("12", 2), LdBigtableUtils.keyStart("12", 1)));
    assertEquals(-1, compareKeys(LdBigtableUtils.keyStart("21"), LdBigtableUtils.keyEnd("21")));
    assertEquals(-1,
        compareKeys(
            LdBigtableUtils.keyStart("15", 10), LdBigtableUtils.keyStart("15", 10, "A", "C")));
    assertEquals(1,
        compareKeys(
            LdBigtableUtils.keyStart("19", 11), LdBigtableUtils.keyEnd("19", 10)));
  }

  /**
   * Returns the comparison of the keys based on their interpretation as twos-complement
   * big-endian integers.
   */
  private static int compareKeys(byte[] key1, byte[] key2) {
    assertEquals(key1.length, KEY_LENGTH);
    assertEquals(key2.length, KEY_LENGTH);
    return new BigInteger(key1).compareTo(new BigInteger(key2));
  }
}
