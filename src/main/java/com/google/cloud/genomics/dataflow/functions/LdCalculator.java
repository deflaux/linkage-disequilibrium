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
package com.google.cloud.genomics.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.genomics.dataflow.model.LdCalculatorPairRequest;
import com.google.cloud.genomics.dataflow.model.LdValue;
import com.google.cloud.genomics.dataflow.model.LdVariant;
import com.google.cloud.genomics.dataflow.utils.LdVariantStreamIterator;
import com.google.cloud.genomics.utils.GenomicsFactory;

import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Computes LD for every variant between two pairs of shards that must be for the same reference,
 * where:
 * 
 * 1. The variant in the target shard starts at or after the one in the in the query shard. 2. The
 * two variants are within window of each other on the same reference. 3. Both variants have some
 * variability.
 *
 * All comparison are outputted twice, swapping the query/target variant, except for comparison with
 * self, which are not output at all.
 */
public class LdCalculator extends DoFn<LdCalculatorPairRequest, LdValue> {
  private final GenomicsFactory.OfflineAuth auth;
  private final long window;
  private final double cutoff;

  public LdCalculator(GenomicsFactory.OfflineAuth auth, long window, double cutoff) {
    this.auth = auth;
    this.window = window;
    this.cutoff = cutoff;
  }

  @Override
  public void processElement(ProcessContext c)
      throws java.io.IOException, java.security.GeneralSecurityException {

    if (!c.element().getQuery().getReferenceName()
        .equals(c.element().getTarget().getReferenceName())) {
      throw new IllegalArgumentException("Shards must be on the same reference.");
    }

    LdVariantStreamIterator queryLdVariantStreamIter =
        new LdVariantStreamIterator(c.element().getQuery(), auth, c.element().getQueryBoundary());
    LdVariantStreamIterator targetLdVariantStreamIter = null;

    // Our working set of variants from the target shard that could be within window of variants
    // read in from the query shard.
    LinkedList<LdVariant> storedVars = new LinkedList<>();

    while (queryLdVariantStreamIter.hasNext()) {
      // Variant to compare to those in storedVars.
      LdVariant queryVar = queryLdVariantStreamIter.next();

      if (targetLdVariantStreamIter == null) {
        targetLdVariantStreamIter = new LdVariantStreamIterator(c.element().getTarget(), auth,
            c.element().getTargetBoundary(), queryLdVariantStreamIter);
      }

      // Fill in storedVars until we are past window from queryVar.
      while (targetLdVariantStreamIter.hasNext() && (storedVars.isEmpty()
          || (queryVar.getInfo().getEnd() + window) > storedVars.getLast().getInfo().getStart())) {
        storedVars.add(targetLdVariantStreamIter.next());
      }

      // Remove variants in storedVars that we are more than window away from and output overlaps.
      ListIterator<LdVariant> varsIter = storedVars.listIterator(0);
      while (varsIter.hasNext()) {
        LdVariant targetVar = varsIter.next();

        // The StreamVariantsResponse Iterator does not have an obvious total ordering (although
        // it does sort by referenceName and start). Thus, there are variants that we may have
        // passed (according to LdVariantInfo.compareTo), but have the same start and we should
        // not remove because the next targetVar may be "before" it.
        //
        // If self-comparisons are desired (e.g. to have a list of the variants with variation)
        // then the compareTo check should be != 1 and the c.output on cr.reverse should only
        // be done if they are not the same variant.
        if (queryVar.getInfo().getStart() > targetVar.getInfo().getStart()) {
          varsIter.remove();
        } else if (queryVar.getInfo().compareTo(targetVar.getInfo()) == -1
            && (queryVar.getInfo().getEnd() + window) > targetVar.getInfo().getStart()) {

          LdValue cr = queryVar.computeLd(targetVar);

          // NaNs are discarded (caused by no variation in one or both variants)
          // TODO: Consider outputting all comparisons (even NaNs) when cutoff < 0.
          // This will require changes to LdVariantStreamIter which discards variants
          // with no variation.
          if (cr.getR() >= cutoff || cr.getR() <= -cutoff) {
            c.output(cr);
            c.output(cr.reverse());
          }
        }
      }
    }
  }
}

