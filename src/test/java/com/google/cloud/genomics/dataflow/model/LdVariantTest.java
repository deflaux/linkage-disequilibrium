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

import com.google.cloud.genomics.dataflow.model.LdVariant.Genotype;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LdVariantTest {
  LdVariantInfo info = new LdVariantInfo("", 0, 0, "", "", 0, 0, "", 0, "");

  @Test
  public void testHasVariation() {
    Assert.assertFalse((new LdVariant(info, new Genotype[] {})).hasVariation());

    Assert.assertFalse((new LdVariant(info, new Genotype[] {Genotype.UNKNOWN})).hasVariation());

    Assert.assertFalse((new LdVariant(info, new Genotype[] {Genotype.ONE})).hasVariation());

    Assert.assertFalse(
        (new LdVariant(info, new Genotype[] {Genotype.ONE, Genotype.ONE})).hasVariation());

    Assert.assertTrue(
        (new LdVariant(info, new Genotype[] {Genotype.ONE, Genotype.ZERO, Genotype.UNKNOWN}))
            .hasVariation());

    Assert.assertFalse(
        (new LdVariant(info, new Genotype[] {Genotype.ONE, Genotype.ONE, Genotype.UNKNOWN}))
            .hasVariation());

    Assert.assertTrue(
        (new LdVariant(info, new Genotype[] {Genotype.ZERO, Genotype.UNKNOWN, Genotype.ONE}))
            .hasVariation());

    Assert.assertFalse(
        (new LdVariant(info, new Genotype[] {Genotype.ZERO, Genotype.UNKNOWN, Genotype.ZERO}))
            .hasVariation());

    Assert.assertFalse(
        (new LdVariant(info, new Genotype[] {Genotype.UNKNOWN, Genotype.UNKNOWN, Genotype.ZERO}))
            .hasVariation());
  }

  private double[] getDPrimeAndR(Genotype[] a, Genotype[] b) {
    LdVariant varA = new LdVariant(info, a);
    LdVariant varB = new LdVariant(info, b);

    LdValue ld = varA.computeLd(varB);

    return new double[] {ld.getDPrime(), ld.getR()};
  }

  private Genotype[] flipGenotypes(Genotype[] a) {
    Genotype[] aFlip = new Genotype[a.length];

    for (int i = 0; i < a.length; i++) {
      aFlip[i] = a[i] == Genotype.ONE ? Genotype.ZERO : a[i] == Genotype.ZERO ? Genotype.ONE : a[i];
    }

    return aFlip;
  }

  // Test that the Genotypes in a and b have dPrime and r when calculated in both orientations and
  // if the meaning of 0 and 1 is flipped in both. Also check that dPrime and r are negated if 
  // only one Genotype is flipped.
  private void testExpectedLdAndReverseAndNeg(double dPrime, double r, Genotype[] a, Genotype[] b) {
    Assert.assertArrayEquals(new double[] {dPrime, r}, getDPrimeAndR(a, b), 1e-6);
    Assert.assertArrayEquals(new double[] {dPrime, r}, getDPrimeAndR(b, a), 1e-6);
    Assert.assertArrayEquals(new double[] {dPrime, r}, getDPrimeAndR(flipGenotypes(b), flipGenotypes(a)), 1e-6);
    Assert.assertArrayEquals(new double[] {-dPrime, -r}, getDPrimeAndR(a, flipGenotypes(b)), 1e-6);
  }

  @Test
  public void testComputeLd() {
    final Genotype ONE = Genotype.ONE;
    final Genotype ZERO = Genotype.ZERO;
    final Genotype UNK = Genotype.UNKNOWN;

    // LD with self should always be 1.
    testExpectedLdAndReverseAndNeg(1.0, 1.0, 
        new Genotype[] {ZERO, ONE},
        new Genotype[] {ZERO, ONE});

    testExpectedLdAndReverseAndNeg(1.0, 1.0, 
        new Genotype[] {ONE, ZERO, ONE},
        new Genotype[] {ONE, ZERO, ONE});

    testExpectedLdAndReverseAndNeg(1.0, 1.0, 
        new Genotype[] {ONE, ZERO, ONE, ONE},
        new Genotype[] {ONE, ZERO, ONE, ONE});

    testExpectedLdAndReverseAndNeg(1.0, 1.0, 
        new Genotype[] {ONE, ZERO, UNK, ONE},
        new Genotype[] {ONE, ZERO, UNK, ONE});

    // Unknown positions must be ignored.
    testExpectedLdAndReverseAndNeg(1.0, 1.0, 
        new Genotype[] {UNK, ZERO, ONE, ONE},
        new Genotype[] {ONE, ZERO, UNK, ONE});

    testExpectedLdAndReverseAndNeg(1, 0.5773503, 
        new Genotype[] {ONE, ONE, ONE, ZERO},
        new Genotype[] {ONE, ONE, ZERO, ZERO});

    testExpectedLdAndReverseAndNeg(1.0, 0.66666667, 
        new Genotype[] {ONE, ONE, ONE, ZERO, ZERO},
        new Genotype[] {ONE, ONE, ZERO, ZERO, ZERO});

    testExpectedLdAndReverseAndNeg(0.3333333, 0.33333333,
        new Genotype[] {ONE, ONE, ONE, ZERO, ZERO, ZERO},
        new Genotype[] {ONE, ONE, ZERO, ZERO, ZERO, ONE});

    testExpectedLdAndReverseAndNeg(1.0, 0.66666667, 
        new Genotype[] {ONE, ONE, ONE, ZERO, ZERO, UNK},
        new Genotype[] {ONE, ONE, ZERO, ZERO, ZERO, ONE});
  }
}
