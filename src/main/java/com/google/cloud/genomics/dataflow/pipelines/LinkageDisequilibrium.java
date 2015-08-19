/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.pipelines;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Proto2Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.readers.VariantStreamer;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.Variant;

/**
 * At the moment this is just a simple variant counting pipeline, intended as an example 
 * for reading data from the Genomics gRPC API.
 */
public class LinkageDisequilibrium {

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(GenomicsDatasetOptions.class);
    GenomicsDatasetOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsDatasetOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);

    final GenomicsFactory.OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerCoder(p, Variant.class,
        SerializableCoder.of(Variant.class));
    DataflowWorkarounds.registerCoder(p, StreamVariantsRequest.class,
        Proto2Coder.of(StreamVariantsRequest.class));

    List<StreamVariantsRequest> requests = options.isAllReferences() ?
        ShardUtils.getVariantRequests(options.getDatasetId(), ShardUtils.SexChromosomeFilter.INCLUDE_XY, options.getBasesPerShard(), auth) :
          ShardUtils.getVariantRequests(options.getDatasetId(), options.getReferences(), options.getBasesPerShard());

    /*
     *  TODO: The shards should actually overlap by K base pairs for LD.  Here's one idea:
     *  
     *  k = 10_000
     *  basesPerShard = 5_000_000 
     *  shardStart = (shardStart - k) > 1 ? shardStart - k : 1
     *  shardEnd = shardStart + basesPerShard + k
     *  
     *  Iterate through requests and update the shard start/end.
     *  
     *  But the pipeline should only emit LD results for SNPs within the original region defined by the shard.
     */
        
        
    p.begin()
    .apply(Create.of(requests))
    .apply(new VariantStreamer(auth, ShardBoundary.Requirement.STRICT, null))
    .apply(Count.<Variant>globally())
    .apply(ParDo.named("FormatResults").of(new DoFn<Long, String>() {
      private static final long serialVersionUID = 0;
      @Override
      public void processElement(ProcessContext c) {
        c.output("Variant Count: " + c.element());
      }
    }))
    .apply(TextIO.Write.to(options.getOutput()));

    p.run();
  }
}
