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

import java.util.BitSet;

/**
 * Container to store variant data needed to compute LD.
 */
public class LdVariant implements java.io.Serializable {
  /**
   * Indicates allele for each chromosome. For multiallelic sites, two alleles should be selected
   * for comparison and marked as ZERO or ONE, with reference as ZERO if it is one of the two
   * alleles. The remaining alleles should be marked as UNKNOWN.
   */
  public enum Genotype {
    UNKNOWN, ZERO, ONE
  }

  private final LdVariantInfo info;

  private final BitSet oneGenotypes;
  private final BitSet missingGenotypes;

  private final int genotypesCount;

  public LdVariant(LdVariantInfo info, Genotype[] genotypes) {
    this.info = info;

    this.oneGenotypes = new BitSet();
    this.missingGenotypes = new BitSet();

    this.genotypesCount = genotypes.length;

    for (int i = 0; i < genotypes.length; i++) {
      if (genotypes[i] == Genotype.ONE) {
        oneGenotypes.set(i);
      } else if (genotypes[i] != Genotype.ZERO) {
        missingGenotypes.set(i);
      }
    }
  }

  public LdVariantInfo getInfo() {
    return info;
  }

  /**
   * Returns true if there are at least two chromosomes with non-missing data whose alleles differ.
   */
  public boolean hasVariation() {
    BitSet oneOrMissingGenotypes = (BitSet) oneGenotypes.clone();
    oneOrMissingGenotypes.or(missingGenotypes);

    return oneGenotypes.nextSetBit(0) != -1
        && oneOrMissingGenotypes.nextClearBit(0) < genotypesCount;
  }

  /**
   * Computes LD between this and that variant. Return zero for both r and D' if the correlation 
   * cannot be computed (note: this can happen even if hasVariation if UNKNOWN values in one lead
   * to insufficient variation in the other).
   */
  public LdValue computeLd(LdVariant that) {
    assert this.genotypesCount == that.genotypesCount;

    BitSet missingInEither = (BitSet) this.missingGenotypes.clone();
    missingInEither.or(that.missingGenotypes);

    BitSet oneInThis = (BitSet) this.oneGenotypes.clone();
    oneInThis.andNot(missingInEither);

    BitSet oneInThat = (BitSet) that.oneGenotypes.clone();
    oneInThat.andNot(missingInEither);

    BitSet oneInBoth = (BitSet) oneInThis.clone();
    oneInBoth.and(oneInThat);

    long n = genotypesCount - missingInEither.cardinality();
    long ab = oneInBoth.cardinality();
    long a = oneInThis.cardinality();
    long b = oneInThat.cardinality();

    if (n == 0 || a == 0 || b == 0 || n==a || n==b) {
      return new LdValue(this.info, that.info, (int) n, (int) a, (int) b, (int) ab, 0.0, 0.0);
    }

    // TODO: Investigate the numerical stability of this computational (everything up until here
    //       should be exact.
    double top = (double) (n * ab - a * b);
    double r = top / Math.sqrt(a * (n - a) * b * (n - b));
    double dPrime =
        top / (top < 0 ? Math.min(a * b, (n - a) * (n - b)) : Math.min(a * (n - b), (n - a) * b));

    return new LdValue(this.info, that.info, (int) n, (int) a, (int) b, (int) ab, r, dPrime);
  }
}
