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

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Converts Variants into LdVariants. Ensures that Variants have the expected callSets. For
 * multiallelic variants, chooses the two alleles with the highest observed frequency. If one of
 * those alleles is the reference, it stays as zeroAllele, otherwise the highest frequency alelle is
 * assigned zeroAllele and the other to oneAllele. Note: does not currently check that the number of
 * genotypes for each callset or the total number of genotypes matches.
 */
public class LdVariantProcessor implements Serializable {
  private final ImmutableMap<String, Integer> callSetsToIndex;

  public LdVariantProcessor(List<String> callSetsNames) {
    this(callSetsNames, null);
  }

  /** 
   * Returns LdVariantProcessor with filtered CallSets.
   * 
   * @param callSetsNames The CallSets for the Variants that will be processed here.
   * @param callSetsToInclude The subset of CallSets to include for the processed LdVariants.
   *    null or empty indicates no filtering.
   */
  public LdVariantProcessor(List<String> callSetsNames, Set<String> callSetsToInclude) {
    int callSetsCount = 0;
    ImmutableMap.Builder<String, Integer> callSetsToIndexBuilder = ImmutableMap.builder();

    if (callSetsToInclude != null && callSetsToInclude.isEmpty()) {
      callSetsToInclude = null;
    }

    for (String cs : callSetsNames) {
      if (callSetsToInclude == null || callSetsToInclude.contains(cs)) {
        callSetsToIndexBuilder.put(cs, callSetsCount++);
      }
    }

    this.callSetsToIndex = callSetsToIndexBuilder.build();

    if (callSetsToInclude != null && callSetsToIndex.size() != callSetsToInclude.size()) {
      throw new IllegalArgumentException("All individuals could not be found in the call set.");
    }
  }

  /**
   * Converts a Variant to an LdVariant, removing information unneeded for computing LD and 
   * redundant between variants. 
   
   * The "zero" and "one" alleles are chosen as the two most used alleles for this variant after 
   * performing filtering. The "zero" allele is the reference if the reference is amongst the top
   * two alleles and otherwise the more abundant allele.
   * 
   * @param var Input Variant.
   * @return Converted LDVariant corresponding to var.
   * @exception IllegalArgumentException if the CallSet does not include all the individuals
   *    stored in callSetsToIndex. NOTE: no check that the number of alleles per CallSet
   *    matches (there is a check that the overall number matches when doing LdVariant.compareTo).
   */
  public LdVariant convertVariant(Variant var) {
    List<VariantCall> calls = var.getCallsList();

    ArrayList<List<Integer>> genotypeLists = new ArrayList<>();
    ArrayList<Integer> genotypes = new ArrayList<>();

    for (int i = 0; i < callSetsToIndex.size(); i++) {
      genotypeLists.add(null);
    }

    for (int i = 0; i < calls.size(); i++) {
      VariantCall vc = calls.get(i);
      Integer callIndex = callSetsToIndex.get(vc.getCallSetName());

      if (callIndex != null) {
        if (genotypeLists.get(callIndex) != null) {
          throw new IllegalArgumentException(
              "Individual " + vc.getCallSetName() + " included more than one time in call set.");
        }

        genotypeLists.set(callIndex, vc.getGenotypeList());
      }
    }

    /* Check that we have genotypes for each individual and flatten genotypeLists into genotypes */
    for (List<Integer> genotypeList : genotypeLists) {
      if (genotypeList == null) {
        throw new IllegalArgumentException("Individual missing from call set.");
      }

      for (Integer genotype : genotypeList) {
        genotypes.add(genotype);
      }
    }

    int zeroAllele = 0;
    int oneAllele = 1;
    if (var.getAlternateBasesCount() > 1) {
      // Multiallelic variant

      int[] genotypeCounts = new int[var.getAlternateBasesCount() + 1];
      for (Integer genotype : genotypes) {
        if (genotype != -1) {
          genotypeCounts[genotype]++;
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

    LdVariant.Genotype[] genotypesConv = new LdVariant.Genotype[genotypes.size()];
    for (int i = 0; i < genotypesConv.length; i++) {
      genotypesConv[i] = (genotypes.get(i) == zeroAllele) ? LdVariant.Genotype.ZERO
          : (genotypes.get(i) == oneAllele) ? LdVariant.Genotype.ONE : LdVariant.Genotype.UNKNOWN;
    }

    return new LdVariant(new LdVariantInfo(var, zeroAllele, oneAllele), genotypesConv);
  }
}

