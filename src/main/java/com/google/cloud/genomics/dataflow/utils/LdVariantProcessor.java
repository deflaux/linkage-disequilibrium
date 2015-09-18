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
package com.google.cloud.genomics.dataflow.utils;

import com.google.cloud.genomics.dataflow.model.LdVariant;
import com.google.cloud.genomics.dataflow.model.LdVariantInfo;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;

import java.util.List;

/**
 * Converts Variants into LdVariants. Ensures that Variants are sorted and have identical CallSets.
 */
public class LdVariantProcessor {
  class CallSetGenotype {
    private final String id;
    private final int genotypeCount;

    public CallSetGenotype(String id, int genotypeCount) {
      this.id = id;
      this.genotypeCount = genotypeCount;
    }

    public String getId() {
      return id;
    }

    public int getGenotypeCount() {
      return genotypeCount;
    }
  }

  private final CallSetGenotype[] callSetGenotypes;
  private final int totalGenotypesCount;

  public LdVariantProcessor(Variant var) {
    List<VariantCall> calls = var.getCallsList();
    int totalGenotypesCount = 0;
    CallSetGenotype[] callSetGenotypes = new CallSetGenotype[calls.size()];
    for (int i = 0; i < calls.size(); i++) {
      callSetGenotypes[i] =
          new CallSetGenotype(calls.get(i).getCallSetId(), calls.get(i).getGenotypeCount());
      totalGenotypesCount += callSetGenotypes[i].genotypeCount;
    }

    this.totalGenotypesCount = totalGenotypesCount;
    this.callSetGenotypes = callSetGenotypes;
  }

  // returns null if there is no variation for this variant
  public LdVariant convertVariant(Variant var) {
    List<VariantCall> calls = var.getCallsList();

    if (callSetGenotypes.length != calls.size()) {
      throw new IllegalArgumentException("Number of variant calls do not match in shard.");
    }

    int[] genotypes = new int[totalGenotypesCount];

    for (int i = 0, j = 0; i < callSetGenotypes.length; i++) {
      VariantCall vc = calls.get(i);

      if (!callSetGenotypes[i].getId().equals(vc.getCallSetId())
          || callSetGenotypes[i].getGenotypeCount() != vc.getGenotypeCount()) {
        throw new IllegalArgumentException("Call sets do not match in shard.");
      }

      for (int k = 0; k < callSetGenotypes[i].getGenotypeCount(); k++, j++) {
        genotypes[j] = vc.getGenotype(k);

        if (genotypes[j] < -1 || genotypes[j] > var.getAlternateBasesCount()) {
          throw new IllegalArgumentException("Genotype outside allowable range.");
        }
      }
    }

    int zeroAllele = 0;
    int oneAllele = 1;
    if (var.getAlternateBasesCount() > 1) {
      // Multiallelic variant

      int[] genotypeCounts = new int[var.getAlternateBasesCount() + 1];
      for (int i = 0; i < genotypes.length; i++) {
        if (genotypes[i] != -1) {
          genotypeCounts[genotypes[i]]++;
        }
      }
      // find the two most used alleles, breaking ties with the earlier allele
      int max1Allele = genotypeCounts[0] >= genotypeCounts[1] ? 0 : 1;
      int max2Allele = 1 - max1Allele;

      for (int i = 2; i <= var.getAlternateBasesCount(); i++) {
        if (genotypeCounts[i] > genotypeCounts[max1Allele]) {
          max2Allele = max1Allele;
          max1Allele = i;
        } else if (genotypeCounts[i] > genotypeCounts[max2Allele]) {
          max2Allele = i;
        }

        if (max1Allele != 0 && max2Allele != 0) {
          zeroAllele = max1Allele;
          oneAllele = max2Allele;
        } else {
          oneAllele = max1Allele == 0 ? max2Allele : max1Allele;
        }
      }
    }

    LdVariant.Genotype[] genotypesConv = new LdVariant.Genotype[genotypes.length];
    for (int i = 0; i < genotypes.length; i++) {
      genotypesConv[i] = (genotypes[i] == zeroAllele) ? LdVariant.Genotype.ZERO
          : (genotypes[i] == oneAllele) ? LdVariant.Genotype.ONE : LdVariant.Genotype.UNKNOWN;
    }

    return new LdVariant(new LdVariantInfo(var, oneAllele, zeroAllele), genotypesConv);
  }
}

