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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.Serializable;

/**
 * Container to store the results of a LD computation.
 */
public class LdValue implements Serializable {
  private final LdVariantInfo query;
  private final LdVariantInfo target;
  private final int compCount;
  private final int queryOneAlleleCount;
  private final int targetOneAlleleCount;
  private final int queryAndTargetOneAlleleCount;
  private final double r;
  private final double dPrime;

  /**
   * Constructor for an LdValue object.
   *
   * @param query An LdVariantInfo object representing the query variant
   * @param target an LdVariantInfo object representing the target variant
   * @param compCount the total number of chromosomes used in the LD calculations
   * @param queryOneAlleleCount the number of chromosomes with the allele coded as 1 in the query
   * @param targetOneAlleleCount the number of chromosomes with the allele coded as 1 in the target
   * @param queryAndTargetOneAlleleCount the number of chromosomes with both query and target 1
   *        alleles
   * @param r the allelic correlation between the query and target variants
   * @param dPrime the D' measure of LD between the query and target variants
   */
  public LdValue(LdVariantInfo query, LdVariantInfo target, int compCount, int queryOneAlleleCount,
      int targetOneAlleleCount, int queryAndTargetOneAlleleCount, double r, double dPrime) {
    this.query = query;
    this.target = target;
    this.compCount = compCount;
    this.queryOneAlleleCount = queryOneAlleleCount;
    this.targetOneAlleleCount = targetOneAlleleCount;
    this.queryAndTargetOneAlleleCount = queryAndTargetOneAlleleCount;
    this.r = r;
    this.dPrime = dPrime;
  }

  public static LdValue fromTokens(String qChrom, int qStart, int qEnd, String qId, String qRsids,
      int qNumAlt, String qZeroAllele, String qOneAllele, String tChrom, int tStart, int tEnd,
      String tId, String tRsids, int tNumAlt, String tZeroAllele, String tOneAllele, int compCount,
      int queryOneAlleleCount, int targetOneAlleleCount, int queryAndTargetOneAlleleCount,
      double r, double dPrime) {
    LdVariantInfo query = new LdVariantInfo(qChrom, qStart, qEnd, qId, qRsids, qNumAlt, qZeroAllele,
        qOneAllele);
    LdVariantInfo target = new LdVariantInfo(tChrom, tStart, tEnd, tId, tRsids, tNumAlt,
        tZeroAllele, tOneAllele);
    return new LdValue(query, target, compCount, queryOneAlleleCount, targetOneAlleleCount,
        queryAndTargetOneAlleleCount, r, dPrime);
  }

  /**
   * Returns the query variant.
   *
   * @return the LdVariantInfo of the query variant
   */
  public LdVariantInfo getQuery() {
    return query;
  }

  /**
   * Returns the target variant.
   *
   * @return the LdVariantInfo of the target variant
   */
  public LdVariantInfo getTarget() {
    return target;
  }

  /**
   * Returns the total number of chromosomes used in the LD calculation.
   *
   * @return the total number of chromosomes used in the LD calculation
   */
  public int getCompCount() {
    return compCount;
  }

  /**
   * Returns the number of chromosomes containing the "1" allele in the query variant.
   *
   * @return the number of 1 alleles in the query variant
   */
  public int getQueryOneAlleleCount() {
    return queryOneAlleleCount;
  }

  /**
   * Returns the number of chromosomes containing the "1" allele in the target variant.
   *
   * @return the number of 1 alleles in the target variant
   */
  public int getTargetOneAlleleCount() {
    return targetOneAlleleCount;
  }

  /**
   * Returns the number of chromosomes containing the "1" allele in both variants.
   *
   * @return the number of chromosomes with 1 alleles in both query and target variants
   */
  public int getQueryAndTargetOneAlleleCount() {
    return queryAndTargetOneAlleleCount;
  }

  /**
   * Returns the allelic correlation between the query and target variants.
   *
   * @return the allelic correlation between the query and target variants
   */
  public double getR() {
    return r;
  }

  /**
   * Returns the D' measure of LD between the query and target variants.
   *
   * @return the D' measure of LD between the query and target variants
   */
  public double getDPrime() {
    return dPrime;
  }

  /**
   * Returns an LdValue object with the query and target variants reversed.
   *
   * @return an LdValue object with the query and target variants reversed
   */
  public LdValue reverse() {
    return new LdValue(target, query, compCount, targetOneAlleleCount, queryOneAlleleCount,
        queryAndTargetOneAlleleCount, r, dPrime);
  }

  /**
   * Returns an LdValue from its String representation.
   *
   * @param line the String line representing the LdValue
   * @return an LdValue object encoded by the line
   */
  public static LdValue fromLine(String line) {
    String[] tokens = line.trim().split(",");
    Preconditions.checkArgument(
        tokens.length == 22,
        "Invalid LdValue line has %s columns (not 22): %s",
        tokens.length,
        line);
    return LdValue.fromTokens(
        tokens[0],
        Integer.parseInt(tokens[1]),
        Integer.parseInt(tokens[2]),
        tokens[3],
        tokens[4],
        Integer.parseInt(tokens[5]),
        tokens[6],
        tokens[7],
        tokens[8],
        Integer.parseInt(tokens[9]),
        Integer.parseInt(tokens[10]),
        tokens[11],
        tokens[12],
        Integer.parseInt(tokens[13]),
        tokens[14],
        tokens[15],
        Integer.parseInt(tokens[16]),
        Integer.parseInt(tokens[17]),
        Integer.parseInt(tokens[18]),
        Integer.parseInt(tokens[19]),
        Double.parseDouble(tokens[20]),
        Double.parseDouble(tokens[21]));
  }

  /**
   * Returns a String representation of this LdValue object.
   *
   * <p>The String representation is a single line containing the following comma-separated values:
   *
   * Values 1-8. Properties of query variant (see LdVariantInfo.toString)
   * 9-16. Properties of target variant
   * 17. Number of chromosomes used in comparison
   * 18. Number of chromosomes used in comparison with one allele for query
   * 19. Number of chromosomes used in comparison with one allele for target
   * 20. Number of chromosomes used in comparison with one allele for both query and target
   * 21. r
   * 22. D'
   *
   * @return a String representing this LdValue object
   */
  public String toString() {
    // NOTE: This format must be kept in sync with the LdValue.fromLine static factory method.
    return Joiner.on(",")
        .join(
            query.toString(),
            target.toString(),
            Integer.toString(compCount),
            Integer.toString(queryOneAlleleCount),
            Integer.toString(targetOneAlleleCount),
            Integer.toString(queryAndTargetOneAlleleCount),
            // TODO: Consider switching to Double.toString for improved precision.
            String.format("%f", r),
            String.format("%f", dPrime));
  }
}
