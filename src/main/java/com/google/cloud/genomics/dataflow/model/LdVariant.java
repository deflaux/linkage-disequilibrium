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
 */
public class LdVariant implements java.io.Serializable {
  public enum Genotype {
    UNKNOWN, ZERO, ONE
  }

  private final LdVariantInfo info;

  private final Genotype[] genotypes;

  public LdVariant(LdVariantInfo info, Genotype[] genotypes) {
    this.info = info;
    this.genotypes = genotypes;
  }

  public LdVariantInfo getInfo() {
    return info;
  }

  /**
   * Returns true if there are at least two chromosomes with non-missing data whose alleles differ.
   */
  public boolean hasVariation() {
    int firstNonMissing;

    for (firstNonMissing = 0; firstNonMissing < genotypes.length
        && genotypes[firstNonMissing] == Genotype.UNKNOWN; firstNonMissing++) {
    };

    for (int i = firstNonMissing + 1; i < genotypes.length; i++) {
      if (genotypes[i] != Genotype.UNKNOWN && genotypes[i] != genotypes[firstNonMissing]) {
        return true;
      }
    }

    return false;
  }

  /**
   * Computes LD between this and target variant.
   */
  public LdValue computeLd(LdVariant target) {
    assert this.genotypes.length == target.genotypes.length;

    /*
     * Compute how often both a and b are Genotype.ONE, or when one or the other is. All LD values
     * can be computed from these simple counts.
     */
    int[][] count = new int[2][2];
    for (int i = 0; i < this.genotypes.length; i++) {
      if (this.genotypes[i] != Genotype.UNKNOWN && target.genotypes[i] != Genotype.UNKNOWN) {
        count[this.genotypes[i] == Genotype.ZERO ? 0 : 1][target.genotypes[i] == Genotype.ZERO ? 0
            : 1]++;
      }
    }

    long n = count[0][0] + count[0][1] + count[1][0] + count[1][1];
    long ab = count[1][1];
    long a = count[1][1] + count[1][0];
    long b = count[1][1] + count[0][1];

    // TODO: Test this throughly, particularly investigate numerical stability.
    double top = (double) (n * ab - a * b);
    double r = top / Math.sqrt(a * (n - a) * b * (n - b));
    double dPrime =
        top / (top < 0 ? Math.min(a * b, (n - a) * (n - b)) : Math.min(a * (n - b), (n - a) * b));

    return new LdValue(this.info, target.info, (int) n, r, dPrime);
  }
}
