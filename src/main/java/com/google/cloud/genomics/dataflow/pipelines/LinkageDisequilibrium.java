/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.services.genomics.model.ReferenceBound;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Proto2Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.functions.LdCreateVariantListsAndAssign;
import com.google.cloud.genomics.dataflow.functions.LdShardToVariantPairs;
import com.google.cloud.genomics.dataflow.model.LdValue;
import com.google.cloud.genomics.dataflow.model.LdVariant;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.GenomicsUtils;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.genomics.v1.StreamVariantsRequest;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

/**
 * Computes linkage disequilibrium r and D' between all variants that start inside the references
 * list if they are within window of each other.
 * 
 * Outputs name1, name2, num, r for those pairs whose r is at least cutoff. For variants with
 * greater than two alleles, the top two alleles are used for the comparison and are appended to the
 * output name.
 * 
 * TODO: Option to work on unphased data.
 * 
 * TODO: Add tests, with special attention to the following cases: 1. When the window size is
 * greater than a shard size, and the converse. 2. Variants that overlap a shard, window, and
 * reference boundary (off-by-one errors, in particular). 3. Multiple variants at the same start
 * and/or end, including overlaps with shard, window, and reference boundaries. 4. 1 bp shards,
 * empty shards, shards past the end of the chromosome.
 */
public class LinkageDisequilibrium {
  /**
   * Additional options for computing LD.
   */
  public interface LinkageDisequilibriumOptions extends GenomicsDatasetOptions {
    @Description("Window to use in computing LD.")
    @Default.Long(1000000L)
    Long getWindow();

    void setWindow(Long window);

    @Description("Linkage disequilibrium r cutoff.")
    @Default.Double(0.2)
    Double getLdCutoff();

    void setLdCutoff(Double ldCutoff);
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    PipelineOptionsFactory.register(LinkageDisequilibriumOptions.class);
    final LinkageDisequilibriumOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(LinkageDisequilibriumOptions.class);
    LinkageDisequilibriumOptions.Methods.validateOptions(options);

    final GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerCoder(p, StreamVariantsRequest.class,
        Proto2Coder.of(StreamVariantsRequest.class));

    List<Contig> contigs = options.isAllReferences()
        ? FluentIterable.from(GenomicsUtils.getReferenceBounds(options.getDatasetId(), auth))
            .transform(new Function<ReferenceBound, Contig>() {
              @Override
              public Contig apply(ReferenceBound bound) {
                return new Contig(bound.getReferenceName(), 0, bound.getUpperBound());
              }
            }).toList()
        : Lists.newArrayList(Contig.parseContigsFromCommandLine(options.getReferences()));

    final double ldCutoff = options.getLdCutoff();
    final long basesPerShard = options.getBasesPerShard();
    final int shardsPerWindow = (int) ((options.getWindow() + basesPerShard - 1) / basesPerShard);

    // Do sharding here manually so we can maintain the original contig and index within contig
    // which we will need later.
    List<KV<KV<Integer, Contig>, KV<Integer, StreamVariantsRequest>>> shards = Lists.newArrayList();
    int contigIndex = 0;
    for (Contig contig : contigs) {
      int shardIndex = 0;
      for (long start = contig.start; start < contig.end; shardIndex++, start += basesPerShard) {
        StreamVariantsRequest shard = StreamVariantsRequest.newBuilder()
            .setVariantSetId(options.getDatasetId()).setReferenceName(contig.referenceName)
            .setStart(start).setEnd(Math.min(contig.end, start + basesPerShard)).build();
        shards.add(KV.of(KV.of(contigIndex, contig), KV.of(shardIndex, shard)));
      }
      contigIndex++;
    }
    // shuffle to spread out requests
    Collections.shuffle(shards);

    // TODO: do something to make the variant processors the same...
    // Iterator<StreamVariantsResponse> vsi = new VariantStreamIterator(shards.get(0).getValue(),
    // auth, ShardBoundary.Requirement.OVERLAPS, null);
    // LdVariantProcessor vp = new LdVariantProcessor(vsi.next().getVariantsList().get(0));

    p.apply(Create.of(shards))
        .apply(ParDo.named("LdCreateVariantListsAndAssign")
            .of(new LdCreateVariantListsAndAssign(auth, basesPerShard, shardsPerWindow)))
        .apply(GroupByKey.<String, KV<Boolean, List<LdVariant>>>create())
        .apply(ParDo.named("LdShardToVariantPairs")
            .of(new LdShardToVariantPairs(options.getWindow(), shardsPerWindow)))
        .apply(ParDo.named("ComputeLd").of(new DoFn<KV<LdVariant, LdVariant>, LdValue>() {
          @Override
          public void processElement(ProcessContext c) {
            c.output(c.element().getKey().computeLd(c.element().getValue()));
          }
        })).apply(ParDo.named("FilterAndReverseLd").of(new DoFn<LdValue, LdValue>() {
          @Override
          public void processElement(ProcessContext c) {
            if (c.element().getR() >= ldCutoff || c.element().getR() <= -ldCutoff) {
              c.output(c.element());
              c.output(c.element().reverse());
            }
          }
        })).apply(ParDo.named("ConvertToString").of(new DoFn<LdValue, String>() {
          @Override
          public void processElement(ProcessContext c) {
            c.output(c.element().toString());
          }
        })).apply(TextIO.Write.withoutSharding().to(options.getOutput()));

    p.run();
  }
}
