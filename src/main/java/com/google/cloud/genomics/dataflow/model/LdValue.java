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
    return String.format("%s,%s,%d,%d,%d,%d,%f,%f", query.toString(), target.toString(), compCount,
        queryOneAlleleCount, targetOneAlleleCount, queryAndTargetOneAlleleCount, r, dPrime);
  }
}

