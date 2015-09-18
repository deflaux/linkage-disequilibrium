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
  private final double r;
  private final double dPrime;

  public LdValue(LdVariantInfo query, LdVariantInfo target, int compCount, double r,
      double dPrime) {
    this.query = query;
    this.target = target;
    this.compCount = compCount;
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

  public double getR() {
    return r;
  }

  public double getDPrime() {
    return dPrime;
  }

  public LdValue reverse() {
    return new LdValue(target, query, compCount, r, dPrime);
  }

  public String toString() {
    return String.format("%s %s %d %f %f", query.toString(), target.toString(), compCount, r, dPrime);
  }
}

