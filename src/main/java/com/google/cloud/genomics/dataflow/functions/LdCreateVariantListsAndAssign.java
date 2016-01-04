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
import com.google.cloud.genomics.dataflow.utils.LdVariantProcessor;
import com.google.cloud.genomics.dataflow.utils.LdVariantStreamIterator;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.common.collect.ImmutableList;
import com.google.genomics.v1.StreamVariantsRequest;

import java.util.List;
import java.util.ListIterator;

/**
 * Part of the LinkageDisequilibrium pipeline that splits all pairwise comparisons of variants
 * within a contig into many smaller comparisons of lists of variants for two regions.
 * 
 * <p>This is achieved by:</p>
 * <ol>
 * <li> Creating two lists of all the variants in the shard: one with "STRICT" and one with
 *    "OVERLAPS" semantics.
 * <li> Emits the lists multiple times with a key such that grouping by the key results in pairs of
 *    lists that cover all comparisons between variants within a Contig exactly once. It is OK to 
 *    comparisons outside of the window because there is an later filter for that, but the same
 *    comparison cannot occur in multiple pairs of variant lists.
 * </ol>
 */
public class LdCreateVariantListsAndAssign extends
    DoFn<KV<KV<Integer, Contig>, KV<Integer, StreamVariantsRequest>>, KV<String, KV<Boolean, List<LdVariant>>>> {

  private final GenomicsFactory.OfflineAuth auth;
  private final long basesPerShard;
  private final int shardsPerWindow;
  private final LdVariantProcessor ldVariantProcessor;

  /**
   * Options needed for the creation and assignment of the LdVariant lists. 
   *
   * @param auth Authorization used for requesting the variants.
   * @param basesPerShard Number of bases that each shard is (for all but the last shard for
   *    each contig this should be the end - start.
   * @param shardsPerWindow The ceiling of the size of the window divided by basesPerShard.
   * @param ldVariantProcessor Processor that takes a Variant and creates an LdVariant removing
   *    unneeded data.
   */
  public LdCreateVariantListsAndAssign(GenomicsFactory.OfflineAuth auth, long basesPerShard,
      int shardsPerWindow, LdVariantProcessor ldVariantProcessor) {
    this.auth = auth;
    this.basesPerShard = basesPerShard;
    this.shardsPerWindow = shardsPerWindow;
    this.ldVariantProcessor = ldVariantProcessor;
  }


  /**
   * Returns list with all variants that start before a specified position removed.
   *
   * @param input List of variants, sorted in ascending order by start.
   * @param startFilter The earliest start position that is accepted.
   * @return Filtered list of variants. If no variants are removed, the original list is returned.
   */
  private List<LdVariant> filterStartLdVariants(List<LdVariant> input, long startFilter) {
    ListIterator<LdVariant> iter = input.listIterator();
    boolean noFiltering = true;
    while (iter.hasNext()) {
      if (iter.next().getInfo().getStart() >= startFilter) {
        iter.previous();
        break;
      }
      noFiltering = false;
    }

    if (noFiltering) {
      return input;
    }

    return ImmutableList.copyOf(iter);
  }

  /** 
   * Takes a contig and shard and emits lists of variants tagged with the other lists of variants
   * they should be compared to in order to (along with all other shards for this contig) cover
   * all pairwise comparisons.
   *
   * <p>
   * The input has two parts, the contig and shard, each of which have an index. The index for the
   * contig (which when running the LD pipeline genome-wide will be a chromosome) is used as part
   * of the tag to ensure that comparisons are all done within a contig. The shard, which is a 
   * subset of the contig, is represented by a StreamVariantsRequest. Because the basesPerShard
   * is known, the index and positions for all other shards can be computed and used to pair the
   * variants in this shard with other shards.
   */
  @Override
  public void processElement(ProcessContext c)
      throws java.io.IOException, java.security.GeneralSecurityException {
    int contigIndex = c.element().getKey().getKey();
    Contig contig = c.element().getKey().getValue();
    int shardIndex = c.element().getValue().getKey();
    StreamVariantsRequest shard = c.element().getValue().getValue();

    int contigShardCount = (int) ((contig.end - contig.start + basesPerShard - 1) / basesPerShard);

    // vars uses "OVERLAPS" boundary semantics -- it includes everything that overlaps a
    // region (even if it is not completely found within the region).
    List<LdVariant> vars;
    for (int attempt = 1;; attempt++) {
      try {
        vars = ImmutableList.copyOf(new LdVariantStreamIterator(shard, auth, ldVariantProcessor));
      } catch (io.grpc.StatusRuntimeException e) {
        if (attempt < 10) {
          continue;
        }
        throw e;
      }
      break;
    }

    // Remove anything from before the start of the contig.
    vars = filterStartLdVariants(vars, contig.start);

    // Also produce "STRICT" boundary semantics -- exclude variants that overlap the base
    // prior to the start of a shard.
    List<LdVariant> varsStrict = filterStartLdVariants(vars, shard.getStart());

    for (int i = 0; i <= shardsPerWindow; i++) {
      /*
       * When the target is the one that is furthest away from the query, we include the variants
       * that overlap the start of the query because they may be within window of variants in
       * target. We don't want to include it in other pairs because then we would do those
       * comparisons more than once (as part of other shards).
       */
      if ((shardIndex + i) < contigShardCount) {
        // true indicates this is the query list for this pair
        c.output(KV.of(String.format("%d:%d:%d", contigIndex, shardIndex, shardIndex + i),
            KV.of(true, (i == shardsPerWindow) ? vars : varsStrict)));
      }

      if ((shardIndex - i) >= 0) {
        c.output(KV.of(String.format("%d:%d:%d", contigIndex, shardIndex - i, shardIndex),
            KV.of(false, varsStrict)));
      }
    }
  }
}

