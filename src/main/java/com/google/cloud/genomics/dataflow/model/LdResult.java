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

import com.google.auto.value.AutoValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;


/**
 * Container class that stores the results of running the LD pipeline.
 */
@AutoValue
public abstract class LdResult {
  // All private instance variables of an LdResult object.
  // Query variant variables
  abstract public String queryChrom();
  abstract public String queryCloudId();
  abstract public String queryNames();
  abstract public String queryZeroAllele();
  abstract public String queryOneAllele();
  abstract public int queryStart();
  abstract public int queryEnd();
  abstract public int queryNumAltAlleles();
  // Target variant variables
  abstract public String targetChrom();
  abstract public String targetCloudId();
  abstract public String targetNames();
  abstract public String targetZeroAllele();
  abstract public String targetOneAllele();
  abstract public int targetStart();
  abstract public int targetEnd();
  abstract public int targetNumAltAlleles();
  // Linkage disequilibrium calculation variables
  abstract public int numChromosomes();
  abstract public int numQueryOneAlleleChromosomes();
  abstract public int numTargetOneAlleleChromosomes();
  abstract public int numQueryAndTargetOneAlleleChromosomes();
  abstract public double allelicCorrelation();
  abstract public double dprime();

  @AutoValue.Builder
  abstract static class Builder {
    // Builder creation functions
    abstract Builder queryChrom(String s);
    abstract Builder queryCloudId(String s);
    abstract Builder queryNames(String s);
    abstract Builder queryZeroAllele(String s);
    abstract Builder queryOneAllele(String s);
    abstract Builder queryStart(int n);
    abstract Builder queryEnd(int n);
    abstract Builder queryNumAltAlleles(int n);
    abstract Builder targetChrom(String s);
    abstract Builder targetCloudId(String s);
    abstract Builder targetNames(String s);
    abstract Builder targetZeroAllele(String s);
    abstract Builder targetOneAllele(String s);
    abstract Builder targetStart(int n);
    abstract Builder targetEnd(int n);
    abstract Builder targetNumAltAlleles(int n);
    abstract Builder numChromosomes(int n);
    abstract Builder numQueryOneAlleleChromosomes(int n);
    abstract Builder numTargetOneAlleleChromosomes(int n);
    abstract Builder numQueryAndTargetOneAlleleChromosomes(int n);
    abstract Builder allelicCorrelation(double d);
    abstract Builder dprime(double d);

    // An automatically-generated build that has no validation.
    abstract LdResult autoBuild();

    /**
     * Returns an LdResult from the Builder.
     *
     * <p>This method both creates an LdResult and validates its inputs.
     */
    public LdResult build() {
      // Create the unvalidated result.
      LdResult res = autoBuild();

      // Ensure the result is valid.
      Preconditions.checkArgument(!res.queryChrom().isEmpty(), "Missing queryChrom");
      Preconditions.checkArgument(res.queryStart() >= 0, "queryStart < 0: %s", res.queryStart());
      Preconditions.checkArgument(
          res.queryEnd() > res.queryStart(),
          "queryEnd (%s) <= queryStart (%s)",
          res.queryEnd(),
          res.queryStart());
      Preconditions.checkArgument(!res.queryCloudId().isEmpty(), "Missing queryCloudId");
      Preconditions.checkArgument(!res.queryNames().isEmpty(), "Missing queryNames");
      Preconditions.checkArgument(
          res.queryNumAltAlleles() > 0,
          "queryNumAltAlleles <= 0: %s",
          res.queryNumAltAlleles());
      Preconditions.checkArgument(!res.queryZeroAllele().isEmpty(), "Missing queryZeroAllele");
      Preconditions.checkArgument(!res.queryOneAllele().isEmpty(), "Missing queryOneAllele");
      Preconditions.checkArgument(!res.targetChrom().isEmpty(), "Missing targetChrom");
      Preconditions.checkArgument(res.targetStart() >= 0, "targetStart < 0: %s", res.targetStart());
      Preconditions.checkArgument(
          res.targetEnd() > res.targetStart(),
          "targetEnd (%s) <= targetStart (%s)",
          res.targetEnd(),
          res.targetStart());
      Preconditions.checkArgument(!res.targetCloudId().isEmpty(), "Missing targetCloudId");
      Preconditions.checkArgument(!res.targetNames().isEmpty(), "Missing targetNames");
      Preconditions.checkArgument(
          res.targetNumAltAlleles() > 0,
          "targetNumAltAlleles <= 0: %s",
          res.targetNumAltAlleles());
      Preconditions.checkArgument(!res.targetZeroAllele().isEmpty(), "Missing targetZeroAllele");
      Preconditions.checkArgument(!res.targetOneAllele().isEmpty(), "Missing targetOneAllele");
      Preconditions.checkArgument(
          res.numChromosomes() > 0,
          "numChromosomes <= 0: %s",
          res.numChromosomes());
      Preconditions.checkArgument(
          res.numQueryOneAlleleChromosomes() >= 0 && res.numQueryOneAlleleChromosomes() <= res.numChromosomes(),
          "Invalid numQueryOneAlleleChromosomes");
      Preconditions.checkArgument(
          res.numTargetOneAlleleChromosomes() >= 0 && res.numTargetOneAlleleChromosomes() <= res.numChromosomes(),
          "Invalid numTargetOneAlleleChromosomes");
      Preconditions.checkArgument(
          res.numQueryAndTargetOneAlleleChromosomes() >= 0
          && res.numQueryAndTargetOneAlleleChromosomes() <= res.numQueryOneAlleleChromosomes()
          && res.numQueryAndTargetOneAlleleChromosomes() <= res.numTargetOneAlleleChromosomes(),
          "Invalid numQueryAndTargetOneAlleleChromosomes");
      Preconditions.checkArgument(
          res.allelicCorrelation() >= -1 && res.allelicCorrelation() <= 1,
          "Invalid allelicCorrelation: %f",
          res.allelicCorrelation());
      Preconditions.checkArgument(
          res.dprime() >= -1 && res.dprime() <= 1,
          "Invalid dprime: %f",
          res.dprime());
      return res;
    }
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
    return new AutoValue_LdResult.Builder()
        .queryChrom(tokens[0])
        .queryStart(Integer.valueOf(tokens[1]))
        .queryEnd(Integer.valueOf(tokens[2]))
        .queryCloudId(tokens[3])
        .queryNames(tokens[4])
        .queryNumAltAlleles(Integer.valueOf(tokens[5]))
        .queryZeroAllele(tokens[6])
        .queryOneAllele(tokens[7])
        .targetChrom(tokens[8])
        .targetStart(Integer.valueOf(tokens[9]))
        .targetEnd(Integer.valueOf(tokens[10]))
        .targetCloudId(tokens[11])
        .targetNames(tokens[12])
        .targetNumAltAlleles(Integer.valueOf(tokens[13]))
        .targetZeroAllele(tokens[14])
        .targetOneAllele(tokens[15])
        .numChromosomes(Integer.valueOf(tokens[16]))
        .numQueryOneAlleleChromosomes(Integer.valueOf(tokens[17]))
        .numTargetOneAlleleChromosomes(Integer.valueOf(tokens[18]))
        .numQueryAndTargetOneAlleleChromosomes(Integer.valueOf(tokens[19]))
        .allelicCorrelation(Double.valueOf(tokens[20]))
        .dprime(Double.valueOf(tokens[21]))
        .build();
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
            queryChrom(),
            Integer.toString(queryStart()),
            Integer.toString(queryEnd()),
            queryCloudId(),
            queryNames(),
            Integer.toString(queryNumAltAlleles()),
            queryZeroAllele(),
            queryOneAllele(),
            targetChrom(),
            Integer.toString(targetStart()),
            Integer.toString(targetEnd()),
            targetCloudId(),
            targetNames(),
            Integer.toString(targetNumAltAlleles()),
            targetZeroAllele(),
            targetOneAllele(),
            Integer.toString(numChromosomes()),
            Integer.toString(numQueryOneAlleleChromosomes()),
            Integer.toString(numTargetOneAlleleChromosomes()),
            Integer.toString(numQueryAndTargetOneAlleleChromosomes()),
            Double.toString(allelicCorrelation()),
            Double.toString(dprime()));
  }
}
