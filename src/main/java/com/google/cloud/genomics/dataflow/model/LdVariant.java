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

/**
 * A small container to store variant data needed to compute LD.
 *
 */
public class LdVariant {
  private final String name;
  private final String id;
  private final String referenceName;
  private final long start;
  private final long end;
  private final byte[] genotypes;

  private final int zeroAllele;
  private final int oneAllele;

  public LdVariant(String id, String referenceName, long start, long end, int zeroAllele,
      int oneAllele, byte[] genotypes, int alternateBasesCount) {
    this.id = id;
    this.referenceName = referenceName;
    this.start = start;
    this.end = end;
    this.zeroAllele = zeroAllele;
    this.oneAllele = oneAllele;
    this.genotypes = genotypes;
    this.name = id + ":" + referenceName + ":" + start + ":" + end
        + ((alternateBasesCount > 1) ? (":" + zeroAllele + ":" + oneAllele) : "");
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

  public byte[] getGenotypes() {
    return genotypes;
  }

  public String getName() {
    return name;
  }
}
