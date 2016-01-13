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

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.genomics.v1.Variant;
import java.io.Serializable;

/**
 * Container that stores position and allele information for a given variant.
 */
public class LdVariantInfo implements Serializable, Comparable<LdVariantInfo> {
  private final String referenceName;
  private final int start;
  private final int end;
  private final String variantId;
  private final String rsIds;

  private final int alternateBasesCount;
  private final String zeroAlleleBases;
  private final String oneAlleleBases;

  /**
   * Constructor for information about an LD variant.
   */
  public LdVariantInfo(String referenceName, int start, int end, String variantId, String rsIds,
      int alternateBasesCount, String zeroAlleleBases, String oneAlleleBases) {
    this.referenceName = referenceName;
    this.start = start;
    this.end = end;
    this.variantId = variantId;
    this.rsIds = rsIds;
    this.alternateBasesCount = alternateBasesCount;
    this.zeroAlleleBases = zeroAlleleBases;
    this.oneAlleleBases = oneAlleleBases;

    Preconditions.checkArgument(
        !this.referenceName.isEmpty(),
        "Missing referenceName: %s", this.toString());
    Preconditions.checkArgument(this.start >= 0, "start < 0: %s", this.start);
    Preconditions.checkArgument(
        this.start < this.end,
        "start (%s) >= end (%s):", this.start, this.end);
    Preconditions.checkArgument(!this.variantId.isEmpty(), "Missing Cloud ID: %s", this.toString());
    Preconditions.checkArgument(
        this.alternateBasesCount > 0,
        "alternateBasesCount <= 0: %s", this.alternateBasesCount);
    Preconditions.checkArgument(
        !this.zeroAlleleBases.isEmpty(),
        "Missing zero allele: %s", this.toString());
    Preconditions.checkArgument(
        !this.oneAlleleBases.isEmpty(),
        "Missing one allele: %s", this.toString());
    Preconditions.checkArgument(
        !this.zeroAlleleBases.equals(this.oneAlleleBases),
        "Identical alleles: %s", this.toString());
  }

  /**
   * Returns an LdVariantInfo from input Variant.
   *
   * @param var Variant to use to obtain reference name, start, end, id, rsid, etc from. Multiple
   *        rsids in the name field are concatenated with the semicolon character; if no names are
   *        provided the name is the empty string.
   * @param zeroAllele Allele to use for zero in correlation (0 is reference)
   * @param oneAllele Allele to use for one in correlation
   */
  public static LdVariantInfo fromVariant(Variant var, int zeroAllele, int oneAllele) {
    Preconditions.checkArgument(
        var.getStart() <= Integer.MAX_VALUE && var.getStart() >= Integer.MIN_VALUE,
        "start is not a valid integer: %s", var.getStart());
    Preconditions.checkArgument(
        var.getEnd() <= Integer.MAX_VALUE && var.getEnd() >= Integer.MIN_VALUE,
        "end is not a valid integer: %s", var.getEnd());
    return new LdVariantInfo(
        var.getReferenceName(),
        (int) var.getStart(),
        (int) var.getEnd(),
        var.getId(),
        String.join(";", var.getNamesList()),
        var.getAlternateBasesCount(),
        zeroAllele == 0 ? var.getReferenceBases() : var.getAlternateBases(zeroAllele - 1),
        oneAllele == 0 ? var.getReferenceBases() : var.getAlternateBases(oneAllele - 1));
  }

  public String getReferenceName() {
    return referenceName;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public String getCloudId() {
    return variantId;
  }

  public String getRsIds() {
    return rsIds;
  }

  public int getAlternateBasesCount() {
    return alternateBasesCount;
  }

  public String getZeroAlleleBases() {
    return zeroAlleleBases;
  }

  public String getOneAlleleBases() {
    return oneAlleleBases;
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
    return String.format("%s,%d,%d,%s,%s,%d,%s,%s", referenceName, start, end, variantId, rsIds,
        alternateBasesCount, zeroAlleleBases, oneAlleleBases);
  }

  public int compareTo(LdVariantInfo that) {
    return ComparisonChain.start()
        .compare(this.referenceName, that.referenceName)
        .compare(this.start, that.start)
        .compare(this.end, that.end)
        .compare(this.variantId, that.variantId)
        .result();
  }

  public boolean equals(LdVariantInfo that) {
    return this.compareTo(that) == 0;
  }
}
