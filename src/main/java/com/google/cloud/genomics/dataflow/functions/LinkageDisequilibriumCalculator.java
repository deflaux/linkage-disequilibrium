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
import com.google.cloud.genomics.dataflow.model.LdValue;
import com.google.cloud.genomics.dataflow.model.LdVariant;
import com.google.cloud.genomics.dataflow.utils.LdVariantProcessor;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Queue;

/**
 * Consumes shards and produces LD between all variants within a given shard and all variants within
 * window. Note: for pairs that are both found within a shard, LD is computed once and output twice.
 * For pairs between shards, LD is computed and outputted once per shard. This allows for processing
 * the data without having special logic for shards at boundaries
 */
public class LinkageDisequilibriumCalculator extends DoFn<StreamVariantsRequest, LdValue> {
  private final GenomicsFactory.OfflineAuth auth;
  private final long window;
  private final double cutoff;

  public LinkageDisequilibriumCalculator(GenomicsFactory.OfflineAuth auth, long window,
      double cutoff) {
    this.auth = auth;
    this.window = window;
    this.cutoff = cutoff;
  }

  @Override
  public void processElement(ProcessContext c)
      throws java.io.IOException, java.security.GeneralSecurityException {

    /*
     * Our "working set" of variants that could overlap future variants. All these must be before or
     * overlapping the shardEnd. Should not contain anything that ends more than window from the
     * start of the previous variant.
     */
    LinkedList vars = new LinkedList<LdVariant>();

    String shardReference = c.element().getReferenceName();

    long shardStart = c.element().getStart();

    // NOTE: actually one past the end
    long shardEnd = c.element().getEnd();

    // Extend shard by window. See note above about which comparisons are computed/output.
    StreamVariantsRequest extendedShard =
        c.element().toBuilder().setStart(shardStart > window ? (shardStart - window) : 0)
            .setEnd(shardEnd + window).build();

    // Use OVERLAPS ShardBoundary.Requirement because it matches the semantics for "within
    // window".
    // However, enforce STRICT semantics are enforced below for which variants we output LD for.
    Iterator<StreamVariantsResponse> streamIter =
        new VariantStreamIterator(extendedShard, auth, ShardBoundary.Requirement.OVERLAPS, null);

    // Variants we have read in from the stream but have not yet processed.
    Queue<Variant> varsToProcess = new LinkedList<Variant>();

    long lastStart = -1;

    LdVariantProcessor vp = null;

    while (!varsToProcess.isEmpty() || streamIter.hasNext()) {
      if (varsToProcess.isEmpty()) {
        varsToProcess.addAll(streamIter.next().getVariantsList());
        continue;
      }

      if (vp == null) {
        vp = new LdVariantProcessor(varsToProcess.peek());
      }

      // Variant to compare to those in vars.
      LdVariant qVar = vp.convertVariant(varsToProcess.remove());

      if (!shardReference.equals(qVar.getInfo().getReferenceName())) {
        throw new IllegalArgumentException("Variant references do not match in shard.");
      }

      if (qVar.getInfo().getStart() < lastStart) {
        throw new IllegalArgumentException("Variants in shard not sorted by start.");
      }

      lastStart = qVar.getInfo().getStart();

      if (!qVar.hasVariation()) {
        continue;
      }

      // Enforce "STRICT" overlaps here.
      if (qVar.getInfo().getStart() >= shardStart) {
        ListIterator<LdVariant> varsIter = vars.listIterator(0);
        while (varsIter.hasNext()) {
          LdVariant tVar = varsIter.next();

          if (tVar.getInfo().getEnd() + window <= qVar.getInfo().getStart()) {
            varsIter.remove();
          } else {
            LdValue cr = qVar.computeLd(tVar);

            // NOTE: NaN's are discarded (caused by no variation in one or more variant)
            if (cr.getR() >= cutoff || cr.getR() <= -cutoff) {
              // qVar must be after shardStart (check before this loop)
              // if also before shardEnd then it is within shard
              if (qVar.getInfo().getStart() < shardEnd) {
                c.output(cr);
              }

              // tVar must be before shardEnd (checked when adding to vars)
              // if also after shardStart then it is within the shard
              if (tVar.getInfo().getStart() >= shardStart) {
                c.output(cr.reverse());
              }

            }
          }
        }
      }

      // Store variants inside the Shard region to compare to variants read in later.
      if (qVar.getInfo().getStart() < shardEnd) {
        vars.add(qVar);
      }
    }
  }
}

