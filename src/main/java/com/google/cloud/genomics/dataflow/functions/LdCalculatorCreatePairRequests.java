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
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.genomics.v1.StreamVariantsRequest;

public class LdCalculatorCreatePairRequests extends DoFn<Contig, LdCalculatorPairRequest> {

  private final String datasetId;
  private final long window;
  private final long basesPerShard;

  public LdCalculatorCreatePairRequests(String datasetId, long window, long basesPerShard) {
    this.datasetId = datasetId;
    this.window = window;
    this.basesPerShard = basesPerShard;
  }

  @Override
  public void processElement(ProcessContext c) {
    for (StreamVariantsRequest query : ShardUtils.getVariantRequests(datasetId,
        c.element().toString(), basesPerShard)) {

      long windowEnd = Math.min(query.getEnd() + window, c.element().end);

      String windowRegion =
          (new Contig(query.getReferenceName(), query.getStart(), windowEnd)).toString();

      for (StreamVariantsRequest target : ShardUtils.getVariantRequests(datasetId, windowRegion,
          basesPerShard)) {

        /*
         * When the target is the one that is furthest away from the query, we include the variants
         * that overlap the start of the query because they may be within window of variants in
         * target. We don't want to include it in other pairs because then we would do those
         * comparisons more than once (as part of other shards).
         * 
         * We exclude this for the first query shard because we do not want to use variants that
         * start before that in our comparisons (this is because it would lead to double counting if
         * the genome was split up and also because it seems to be considerably more complex in this
         * framework). Note: this doesn't effect "whole genome" style analysis because there is
         * nothing that starts prior to base 0 for each reference.
         */
        boolean includeStartOverlaps =
            target.getEnd() == windowEnd && query.getStart() != c.element().start;
        c.output(new LdCalculatorPairRequest(query, includeStartOverlaps
            ? ShardBoundary.Requirement.OVERLAPS : ShardBoundary.Requirement.STRICT, target,
            ShardBoundary.Requirement.STRICT));
      }
    }
  }
}

