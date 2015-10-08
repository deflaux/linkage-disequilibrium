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
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.model.LdVariant;
import com.google.cloud.genomics.dataflow.utils.LdVariantStreamIterator;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.common.collect.ImmutableList;
import com.google.genomics.v1.StreamVariantsRequest;

import java.util.List;
import java.util.ListIterator;

/**
 */
public class LdCreateVariantListsAndAssign extends
    DoFn<KV<KV<Integer, Contig>, KV<Integer, StreamVariantsRequest>>, KV<String, KV<Boolean, List<LdVariant>>>> {

  private final GenomicsFactory.OfflineAuth auth;
  private final long basesPerShard;
  private final int shardsPerWindow;

  public LdCreateVariantListsAndAssign(GenomicsFactory.OfflineAuth auth, long basesPerShard,
      int shardsPerWindow) {
    this.auth = auth;
    this.basesPerShard = basesPerShard;
    this.shardsPerWindow = shardsPerWindow;
  }

  @Override
  public void processElement(ProcessContext c)
      throws java.io.IOException, java.security.GeneralSecurityException {
    int contigIndex = c.element().getKey().getKey();
    Contig contig = c.element().getKey().getValue();
    int shardIndex = c.element().getValue().getKey();
    StreamVariantsRequest shard = c.element().getValue().getValue();

    int contigShardCount = (int) ((contig.end - contig.start + basesPerShard - 1) / basesPerShard);

    // vars uses "OVERLAPS" boundary semantics -- it includes everything that overlaps a
    // region (even if it is not completely found within the region)
    List<LdVariant> vars;
    for (int attempt = 1;; attempt++) {
      try {
        vars = ImmutableList
            .copyOf(new LdVariantStreamIterator(shard, auth, ShardBoundary.Requirement.OVERLAPS));
      } catch (io.grpc.StatusRuntimeException e) {
        if (attempt < 10) {
          continue;
        }
        throw e;
      }
      break;
    }

    // Also produce "STRICT" boundary semantics -- exclude variants that overlap the base
    // prior to the start of a shard.
    ListIterator<LdVariant> varsIter = vars.listIterator();
    while (varsIter.hasNext()) {
      if (varsIter.next().getInfo().getStart() >= shard.getStart()) {
        varsIter.previous();
        break;
      }
    }
    List<LdVariant> varsStrict = ImmutableList.copyOf(varsIter);

    for (int i = 0; i <= shardsPerWindow; i++) {
      /*
       * When the target is the one that is furthest away from the query, we include the variants
       * that overlap the start of the query because they may be within window of variants in
       * target. We don't want to include it in other pairs because then we would do those
       * comparisons more than once (as part of other shards).
       * 
       * We exclude this for the first query shard because we do not want to use variants that start
       * before that in our comparisons (this is because it would lead to double counting if the
       * genome was split up and also because it seems to be considerably more complex in this
       * framework). Note: this doesn't effect "whole genome" style analysis because there is
       * nothing that starts prior to base 0 for each reference.
       */
      if ((shardIndex + i) < contigShardCount) {
        // true indicates this is the query list for this pair
        c.output(KV.of(String.format("%d:%d:%d", contigIndex, shardIndex, shardIndex + i),
            KV.of(true, (shardIndex == 0 || i != shardsPerWindow) ? varsStrict : vars)));
      }

      if ((shardIndex - i) >= 0) {
        c.output(KV.of(String.format("%d:%d:%d", contigIndex, shardIndex - i, shardIndex),
            KV.of(false, varsStrict)));
      }
    }
  }
}

