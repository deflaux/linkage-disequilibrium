/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.model;

import com.google.common.collect.ComparisonChain;
import com.google.genomics.v1.Variant;

import java.io.Serializable;

/**
 * Container that stores position and allele information for a given variant.
 */
public class LdVariantInfo implements Serializable, Comparable<LdVariantInfo> {
  private final String referenceName;
  private final long start;
  private final long end;
  private final String cloudId;
  private final String rsIds;

  private final int alternateBasesCount;
  private final int zeroAllele;
  private final String zeroAlleleBases;
  private final int oneAllele;
  private final String oneAlleleBases;

  /**
   * Initializes LdVariantInfo from input Variant.
   *
   * @param var Variant to use to obtain reference name, start, end, id, rsid, etc from.
   *            The first value in the name field is used as the rsid (blank of none available).
   * @param zeroAllele Allele to use for zero in correlation (0 is reference)
   * @param oneAllele Allele to use for one in correlation
   */
  public LdVariantInfo(Variant var, int zeroAllele, int oneAllele) {
    this(
      var.getReferenceName(),
      var.getStart(),
      var.getEnd(),
      var.getId(),
      var.getNamesCount() >= 1 ? var.getNames(0) : "",
      var.getAlternateBasesCount(),
      zeroAllele,
      zeroAllele == 0 ? var.getReferenceBases() : var.getAlternateBases(zeroAllele - 1),
      oneAllele,
      oneAllele == 0 ? var.getReferenceBases() : var.getAlternateBases(oneAllele - 1));
  }

  public LdVariantInfo(String referenceName, long start, long end, String cloudId, String rsIds,
      int alternateBasesCount, int zeroAllele, String zeroAlleleBases, int oneAllele,
      String oneAlleleBases) {
    this.referenceName = referenceName;
    this.start = start;
    this.end = end;
    this.cloudId = cloudId;
    this.rsIds = rsIds;
    this.alternateBasesCount = alternateBasesCount;
    this.zeroAllele = zeroAllele;
    this.zeroAlleleBases = zeroAlleleBases;
    this.oneAllele = oneAllele;
    this.oneAlleleBases = oneAlleleBases;
  }

  public String getCloudId() {
    return cloudId;
  }

  public String getRsIds() {
    return rsIds;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public int getZeroAllele() {
    return zeroAllele;
  }

  public int getOneAllele() {
    return oneAllele;
  }

  /**
   * Converts LdVariantInfo to a string.
   * 
   * Output format (separated by commas)
   * 1. chromosome
   * 2. start position (0-based inclusive)
   * 3. end position (0-based exclusive)
   * 4. Cloud Genomics ID
   * 5. Semicolon-separated list of rsids
   * 6. The number of alternative alleles
   * 7. The bases corresponding to the allele represented as 0 in the LD calculation
   * 8. The bases corresponding to the allele represented as 1 in the LD calculation
   */
  public String toString() {
    return String.format("%s,%d,%d,%s,%s,%d,%s,%s", referenceName, start, end, cloudId, rsIds, alternateBasesCount,
        zeroAlleleBases, oneAlleleBases);
  }

  public int compareTo(LdVariantInfo that) {
    return ComparisonChain.start().compare(this.referenceName, that.referenceName)
        .compare(this.start, that.start).compare(this.end, that.end).compare(this.cloudId, that.cloudId)
        .result();
  }

  public boolean equals(LdVariantInfo that) {
    return this.compareTo(that) == 0;
  }

}
