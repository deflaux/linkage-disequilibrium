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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.lang.IllegalStateException;

/**
 * Container class that stores the results of running the LD pipeline.
 */
public class LdResult {
  // All private instance variables of an LdResult object.
  // This class should be an AutoValue, but compilation errors preclude that
  // (see https://b.corp.google.com/u/0/issues/26338428 for details).
  // Query variant variables
  private String queryChrom;
  private String queryCloudId;
  private String queryNames;
  private String queryZeroAllele;
  private String queryOneAllele;
  private int queryStart;
  private int queryEnd;
  private int queryNumAltAlleles;
  // Target variant variables
  private String targetChrom;
  private String targetCloudId;
  private String targetNames;
  private String targetZeroAllele;
  private String targetOneAllele;
  private int targetStart;
  private int targetEnd;
  private int targetNumAltAlleles;
  // Linkage disequilibrium calculation variables
  private int numChromosomes;
  private int numQueryOneAlleleChromosomes;
  private int numTargetOneAlleleChromosomes;
  private int numQueryAndTargetOneAlleleChromosomes;
  private double allelicCorrelation;
  private double dprime;

  /**
   * Constructor for an LdResult object.
   *
   * <p>
   * This function both initializes an object and ensures its inputs are valid.
   */
  public LdResult(String queryChrom, int queryStart, int queryEnd, String queryCloudId,
      String queryNames, int queryNumAltAlleles, String queryZeroAllele, String queryOneAllele,
      String targetChrom, int targetStart, int targetEnd, String targetCloudId,
      String targetNames, int targetNumAltAlleles, String targetZeroAllele,
      String targetOneAllele, int numChromosomes, int numQueryOneAlleleChromosomes,
      int numTargetOneAlleleChromosomes, int numQueryAndTargetOneAlleleChromosomes,
      double allelicCorrelation, double dprime) {
    Preconditions.checkArgument(!queryChrom.isEmpty(), "Missing queryChrom");
    Preconditions.checkArgument(queryStart >= 0, "queryStart < 0: %s", queryStart);
    Preconditions.checkArgument(
        queryEnd > queryStart,
        "queryEnd (%s) <= queryStart (%s)",
        queryEnd,
        queryStart);
    Preconditions.checkArgument(!queryCloudId.isEmpty(), "Missing queryCloudId");
    Preconditions.checkArgument(!queryNames.isEmpty(), "Missing queryNames");
    Preconditions.checkArgument(
        queryNumAltAlleles > 0,
        "queryNumAltAlleles <= 0: %s",
        queryNumAltAlleles);
    Preconditions.checkArgument(!queryZeroAllele.isEmpty(), "Missing queryZeroAllele");
    Preconditions.checkArgument(!queryOneAllele.isEmpty(), "Missing queryOneAllele");
    Preconditions.checkArgument(!targetChrom.isEmpty(), "Missing targetChrom");
    Preconditions.checkArgument(targetStart >= 0, "targetStart < 0: %s", targetStart);
    Preconditions.checkArgument(
        targetEnd > targetStart,
        "targetEnd (%s) <= targetStart (%s)",
        targetEnd,
        targetStart);
    Preconditions.checkArgument(!targetCloudId.isEmpty(), "Missing targetCloudId");
    Preconditions.checkArgument(!targetNames.isEmpty(), "Missing targetNames");
    Preconditions.checkArgument(
        targetNumAltAlleles > 0,
        "targetNumAltAlleles <= 0: %s",
        targetNumAltAlleles);
    Preconditions.checkArgument(!targetZeroAllele.isEmpty(), "Missing targetZeroAllele");
    Preconditions.checkArgument(!targetOneAllele.isEmpty(), "Missing targetOneAllele");
    Preconditions.checkArgument(numChromosomes > 0, "numChromosomes <= 0: %s", numChromosomes);
    Preconditions.checkArgument(
        numQueryOneAlleleChromosomes >= 0 && numQueryOneAlleleChromosomes <= numChromosomes,
        "Invalid numQueryOneAlleleChromosomes");
    Preconditions.checkArgument(
        numTargetOneAlleleChromosomes >= 0 && numTargetOneAlleleChromosomes <= numChromosomes,
        "Invalid numTargetOneAlleleChromosomes");
    Preconditions.checkArgument(
        numQueryAndTargetOneAlleleChromosomes >= 0
        && numQueryAndTargetOneAlleleChromosomes <= numQueryOneAlleleChromosomes
        && numQueryAndTargetOneAlleleChromosomes <= numTargetOneAlleleChromosomes,
        "Invalid numQueryAndTargetOneAlleleChromosomes");
    Preconditions.checkArgument(
        allelicCorrelation >= -1 && allelicCorrelation <= 1,
        "Invalid allelicCorrelation: %f",
        allelicCorrelation);
    Preconditions.checkArgument(
        dprime >= -1 && dprime <= 1,
        "Invalid dprime: %f",
        dprime);
    this.queryChrom = queryChrom;
    this.queryStart = queryStart;
    this.queryEnd = queryEnd;
    this.queryCloudId = queryCloudId;
    this.queryNames = queryNames;
    this.queryNumAltAlleles = queryNumAltAlleles;
    this.queryZeroAllele = queryZeroAllele;
    this.queryOneAllele = queryOneAllele;
    this.targetChrom = targetChrom;
    this.targetStart = targetStart;
    this.targetEnd = targetEnd;
    this.targetCloudId = targetCloudId;
    this.targetNames = targetNames;
    this.targetNumAltAlleles = targetNumAltAlleles;
    this.targetZeroAllele = targetZeroAllele;
    this.targetOneAllele = targetOneAllele;
    this.numChromosomes = numChromosomes;
    this.numQueryOneAlleleChromosomes = numQueryOneAlleleChromosomes;
    this.numTargetOneAlleleChromosomes = numTargetOneAlleleChromosomes;
    this.numQueryAndTargetOneAlleleChromosomes = numQueryAndTargetOneAlleleChromosomes;
    this.allelicCorrelation = allelicCorrelation;
    this.dprime = dprime;
  }

  /**
   * Factory method to create an LdResult object from the output of the LinkageDisequilibrium
   * pipeline.
   */
  public static LdResult fromLine(String line) {
    String[] tokens = line.trim().split(",");
    Preconditions.checkArgument(
        tokens.length == 22,
        "Invalid LdResult line has %s columns (not 22): %s",
        tokens.length,
        line);
    return new LdResult(
        tokens[0],
        Integer.valueOf(tokens[1]),
        Integer.valueOf(tokens[2]),
        tokens[3],
        tokens[4],
        Integer.valueOf(tokens[5]),
        tokens[6],
        tokens[7],
        tokens[8],
        Integer.valueOf(tokens[9]),
        Integer.valueOf(tokens[10]),
        tokens[11],
        tokens[12],
        Integer.valueOf(tokens[13]),
        tokens[14],
        tokens[15],
        Integer.valueOf(tokens[16]),
        Integer.valueOf(tokens[17]),
        Integer.valueOf(tokens[18]),
        Integer.valueOf(tokens[19]),
        Double.valueOf(tokens[20]),
        Double.valueOf(tokens[21]));
  }

  public String queryChrom() {
    return queryChrom;
  }
  public int queryStart() {
    return queryStart;
  }
  public int queryEnd() {
    return queryEnd;
  }
  public String queryCloudId() {
    return queryCloudId;
  }
  public String queryNames() {
    return queryNames;
  }
  public int queryNumAltAlleles() {
    return queryNumAltAlleles;
  }
  public String queryZeroAllele() {
    return queryZeroAllele;
  }
  public String queryOneAllele() {
    return queryOneAllele;
  }
  public String targetChrom() {
    return targetChrom;
  }
  public int targetStart() {
    return targetStart;
  }
  public int targetEnd() {
    return targetEnd;
  }
  public String targetCloudId() {
    return targetCloudId;
  }
  public String targetNames() {
    return targetNames;
  }
  public int targetNumAltAlleles() {
    return targetNumAltAlleles;
  }
  public String targetZeroAllele() {
    return targetZeroAllele;
  }
  public String targetOneAllele() {
    return targetOneAllele;
  }
  public int numChromosomes() {
    return numChromosomes;
  }
  public int numQueryOneAlleleChromosomes() {
    return numQueryOneAlleleChromosomes;
  }
  public int numTargetOneAlleleChromosomes() {
    return numTargetOneAlleleChromosomes;
  }
  public int numQueryAndTargetOneAlleleChromosomes() {
    return numQueryAndTargetOneAlleleChromosomes;
  }
  public double allelicCorrelation() {
    return allelicCorrelation;
  }
  public double dprime() {
    return dprime;
  }

  /**
   * Returns a String representation of the LdResult.
   *
   * <p>
   * This representation is compatible with the fromLine factory method.
   */
  public String toString() {
    return Joiner.on(",")
        .join(
            queryChrom,
            Integer.toString(queryStart),
            Integer.toString(queryEnd),
            queryCloudId,
            queryNames,
            Integer.toString(queryNumAltAlleles),
            queryZeroAllele,
            queryOneAllele,
            targetChrom,
            Integer.toString(targetStart),
            Integer.toString(targetEnd),
            targetCloudId,
            targetNames,
            Integer.toString(targetNumAltAlleles),
            targetZeroAllele,
            targetOneAllele,
            Integer.toString(numChromosomes),
            Integer.toString(numQueryOneAlleleChromosomes),
            Integer.toString(numTargetOneAlleleChromosomes),
            Integer.toString(numQueryAndTargetOneAlleleChromosomes),
            Double.toString(allelicCorrelation),
            Double.toString(dprime));
  }
}
