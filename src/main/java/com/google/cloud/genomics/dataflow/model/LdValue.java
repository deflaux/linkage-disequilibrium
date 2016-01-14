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

  public LdVariantInfo getQuery() {
    return query;
  }

  public LdVariantInfo getTarget() {
    return target;
  }

  public int getCompCount() {
    return compCount;
  }

  public int getQueryOneAlleleCount() {
    return queryOneAlleleCount;
  }

  public int getTargetOneAlleleCount() {
    return targetOneAlleleCount;
  }

  public int getQueryAndTargetOneAlleleCount() {
    return queryAndTargetOneAlleleCount;
  }

  public double getR() {
    return r;
  }

  public double getDPrime() {
    return dPrime;
  }

  /**
   * Returns an LdValue with the query and target reversed.
   */
  public LdValue reverse() {
    return new LdValue(target, query, compCount, targetOneAlleleCount, queryOneAlleleCount,
        queryAndTargetOneAlleleCount, r, dPrime);
  }

  /**
   * Returns an LdValue from its String representation.
   */
  public static LdValue fromLine(String line) {
    String[] tokens = line.trim().split(",");
    Preconditions.checkArgument(
        tokens.length == 22,
        "Invalid LdValue line has %s columns (not 22): %s",
        tokens.length,
        line);
    LdVariantInfo query = new LdVariantInfo(
        tokens[0],
        Integer.parseInt(tokens[1]),
        Integer.parseInt(tokens[2]),
        tokens[3],
        tokens[4],
        Integer.parseInt(tokens[5]),
        tokens[6],
        tokens[7]);
    LdVariantInfo target = new LdVariantInfo(
        tokens[8],
        Integer.parseInt(tokens[9]),
        Integer.parseInt(tokens[10]),
        tokens[11],
        tokens[12],
        Integer.parseInt(tokens[13]),
        tokens[14],
        tokens[15]);
    return new LdValue(
        query,
        target,
        Integer.parseInt(tokens[16]),
        Integer.parseInt(tokens[17]),
        Integer.parseInt(tokens[18]),
        Integer.parseInt(tokens[19]),
        Double.parseDouble(tokens[20]),
        Double.parseDouble(tokens[21]));
  }

  /**
   * Converts this LdValue to a string, using the following format (separated by commas):
   *
   * 1-8. Properties of query variant (see LdVariantInfo.toString)
   * 9-16. Properties of target variant
   * 17. Number of chromosomes used in comparison
   * 18. Number of chromosomes used in comparison with one allele for query
   * 19. Number of chromosomes used in comparison with one allele for target
   * 20. Number of chromosomes used in comparison with one allele for both query and target
   * 21. r
   * 22. D'
   */
  public String toString() {
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
