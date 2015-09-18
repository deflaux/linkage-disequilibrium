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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Proto2Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.functions.LinkageDisequilibriumCalculator;
import com.google.cloud.genomics.dataflow.model.LdValue;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.Variant;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * Computes linkage disequilibrium r between all variants inside the references list of a dataset to
 * all others within window. Outputs name1, name2, num, r for those pairs whose r is at least
 * cutoff. For variants with greater than two alleles, the top two alleles are used for the
 * comparison and are appended to the output name.
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
    LinkageDisequilibriumOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(LinkageDisequilibriumOptions.class);
    LinkageDisequilibriumOptions.Methods.validateOptions(options);

    final GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerCoder(p, Variant.class, SerializableCoder.of(Variant.class));
    DataflowWorkarounds.registerCoder(p, StreamVariantsRequest.class,
        Proto2Coder.of(StreamVariantsRequest.class));

    List<StreamVariantsRequest> requests = options.isAllReferences()
        ? ShardUtils.getVariantRequests(options.getDatasetId(),
            ShardUtils.SexChromosomeFilter.INCLUDE_XY, options.getBasesPerShard(), auth)
        : ShardUtils.getVariantRequests(options.getDatasetId(), options.getReferences(),
            options.getBasesPerShard());

    p.begin().apply(Create.of(requests))
        .apply(ParDo.named("ComputeLD")
            .of(new LinkageDisequilibriumCalculator(auth, options.getWindow(),
                options.getLdCutoff())))
        .apply(ParDo.named("ConvertToString").of(new DoFn<LdValue, String>() {
          @Override
          public void processElement(ProcessContext c) {
            c.output(c.element().toString());
          }
        })).apply(TextIO.Write.withoutSharding().to(options.getOutput()));

    p.run();
  }
}
