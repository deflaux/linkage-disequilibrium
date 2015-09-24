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

import com.google.genomics.v1.Variant;

import java.io.Serializable;

/**
 * Position and allele information for a given variant.
 */
public class LdVariantInfo implements Serializable {
  private final String id;
  private final String referenceName;
  private final long start;
  private final long end;

  private final int alternateBasesCount;
  private final int zeroAllele;
  private final String zeroAlleleBases;
  private final int oneAllele;
  private final String oneAlleleBases;

  public LdVariantInfo(Variant var, int zeroAllele, int oneAllele) {
    this(var.getId(), var.getReferenceName(), var.getStart(), var.getEnd(),
        var.getAlternateBasesCount(), zeroAllele,
        zeroAllele == 0 ? var.getReferenceBases() : var.getAlternateBases(zeroAllele - 1),
        oneAllele, oneAllele == 0 ? var.getReferenceBases() : var.getAlternateBases(oneAllele - 1));
  }

  public LdVariantInfo(String id, String referenceName, long start, long end,
      int alternateBasesCount, int zeroAllele, String zeroAlleleBases, int oneAllele,
      String oneAlleleBases) {
    this.id = id;
    this.referenceName = referenceName;
    this.start = start;
    this.end = end;
    this.alternateBasesCount = alternateBasesCount;
    this.zeroAllele = zeroAllele;
    this.zeroAlleleBases = zeroAlleleBases;
    this.oneAllele = oneAllele;
    this.oneAlleleBases = oneAlleleBases;
  }

  public String getId() {
    return id;
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

  public String toString() {
    return String.format("%s:%s:%d:%d", id, referenceName, start, end) + ((alternateBasesCount > 1)
        ? String.format(":%d:%s:%d:%s", zeroAllele, zeroAlleleBases, oneAllele, oneAlleleBases)
        : "");
  }
}
