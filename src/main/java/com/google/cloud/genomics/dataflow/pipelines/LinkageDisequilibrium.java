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
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.lang.IllegalArgumentException;

/**
 * TODO: write program functionality
 */
public class LinkageDisequilibrium {

  private static class ProcessVariants extends DoFn<StreamVariantsRequest, String> {
    private final GenomicsFactory.OfflineAuth auth;
    private final long window;
    private final double cutoff;

    public ProcessVariants(GenomicsFactory.OfflineAuth auth, long window, double cutoff) {
      this.auth = auth;
      this.window = window;
      this.cutoff = cutoff;
    }

    private double computeLd(List<VariantCall> aVar, int aRefGenotype, List<VariantCall> bVar,
        int bRefGenotype) {

      if (aVar.size() != bVar.size()) {
        throw new IllegalArgumentException("Inconsistent number of variant calls.");
      }

      double[][] tempValues = new double[2][2 * aVar.size()];
      Iterator<VariantCall> aIter = aVar.iterator();
      Iterator<VariantCall> bIter = bVar.iterator();

      int[] refGenotypes = {aRefGenotype, bRefGenotype};

      int n = 0;

      while (aIter.hasNext() && bIter.hasNext()) {
        VariantCall[] calls = {aIter.next(), bIter.next()};

        if (!calls[0].getCallSetId().equals(calls[1].getCallSetId())
            || calls[0].getGenotypeCount() != calls[1].getGenotypeCount()
            || calls[0].getGenotypeCount() > 2) {
          throw new IllegalArgumentException("Call set mismatch.");
        }

        for (int g = 0; g < calls[0].getGenotypeCount(); g++) {
          if (calls[0].getGenotype(g) == -1 || calls[1].getGenotype(g) == -1) {
            continue;
          }

          for (int i = 0; i < 2; i++) {
            tempValues[i][n] = calls[i].getGenotype(g) == refGenotypes[g] ? 0 : 1;
          }

          n++;
        }
      }

      // Should always match because of size check above.
      assert aIter.hasNext() == bIter.hasNext();

      double[][] values = new double[2][n];

      for (int i = 0; i < 2; i++) {
        System.arraycopy(tempValues[i], 0, values[i], 0, n);
      }

      return (new PearsonsCorrelation()).correlation(values[0], values[1]);
    }

    /*
     * Print all within shard/reference and variants within window. NOTE: this means that
     * comparisons where both variants are within a shard will be computed once and outputted twice,
     * whereas those across shards will be computed twice and outputted once per shard.
     */

    // 1. Create new StreamVariantsRequest with window -- consider using non-STRICT boundary
    // requirement
    // keep track of original start/end (output only strictly within boundary)
    // Repeat until list is empty:
    // Read new value
    // Scan through list until cur
    // If iv > window of cv
    // Remove
    // else
    // Compute LD against list.end and output
    // if cv is past end ----> delete it


    // TODO: tests -- window > shard, shard > window, variants overlapping shard boundary, window
    // boundary.
    // reference boundary. two variants both overlapping the same shard boundary
    // 1bp shards, shards that go to 0, past end of chromosome, shards with no variants

    private static String variantRefToName(Variant var, int refGenotype) {
      return var.getId() + ":" + var.getReferenceName() + ":" + var.getStart() + ":" + var.getEnd() + ((var.getAlternateBasesCount() > 1) ? (":" + refGenotype) : "");
    }

    @Override
    public void processElement(ProcessContext c)
        throws java.io.IOException, java.security.GeneralSecurityException {
      /*
       * Our "working set" of variants that could overlap future variants. All these must be before
       * or overlapping the shardEnd. Should not contain anything that ends more than window from
       * the start of the previous variant.
       */
      LinkedList vars = new LinkedList<Variant>();

      // NOTE: if chromosome then need way to compare order of chromosomes
      long shardStart = c.element().getStart();

      // NOTE: actually one past the end
      long shardEnd = c.element().getEnd();

      // Extend shard by window. See note above about which comparisons are
      // computed/output.
      StreamVariantsRequest extendedShard =
          c.element().toBuilder().setStart(shardStart > window ? (shardStart - window) : 0)
              .setEnd(shardEnd + window).build();

      // Use OVERLAPS because it matches the semantics for "within window"
      // However, comparisons note uses STRICT semantics.
      Iterator<StreamVariantsResponse> streamIter =
          new VariantStreamIterator(extendedShard, auth, ShardBoundary.Requirement.OVERLAPS, null);

      // Variants we have read in from the stream but have not yet processed.
      Queue<Variant> varsToProcess = new LinkedList<Variant>();

      Variant cVar = null;
      while (!varsToProcess.isEmpty() || streamIter.hasNext()) {
        if (varsToProcess.isEmpty()) {
          varsToProcess.addAll(streamIter.next().getVariantsList());
          continue;
        }

        // Make sure that the next variant is on the same reference and does not
        // start before this one.
        assert cVar == null
            || (cVar.getReferenceName().equals(varsToProcess.peek().getReferenceName())
                && cVar.getStart() <= varsToProcess.peek().getStart());

        // Variant to compare to those in vars.
        cVar = varsToProcess.remove();

        // Manually enforce "STRICT" overlaps here.
        if (cVar.getStart() >= shardStart) {
          ListIterator<Variant> varsIter = vars.listIterator(0);
          while (varsIter.hasNext()) {
            Variant lVar = varsIter.next();

            // NOTE: end is one past end
            if (lVar.getEnd() + window <= cVar.getStart()) {
              varsIter.remove();
            } else {
              // If the number of alternative bases is > 1, then we do each allele vs. all others.
              for (int lRefGenotype = 0; lRefGenotype <= ((lVar.getAlternateBasesCount() > 1) ? lVar.getAlternateBasesCount() : 0); lRefGenotype++) {
                for (int cRefGenotype = 0; cRefGenotype <= ((cVar.getAlternateBasesCount() > 1) ? cVar.getAlternateBasesCount() : 0); cRefGenotype++) {

                  double corr = computeLd(lVar.getCallsList(), lRefGenotype, cVar.getCallsList(), cRefGenotype);

                  // NOTE: NaN's are discarded (caused by no variation in one or more variant)
                  if (corr >= cutoff || corr <= -cutoff) {
                    // lVar must be before shardEnd (checked when adding to vars)
                    // if also after shardStart then it is within the shard
                    if (lVar.getStart() >= shardStart) {
                      c.output(variantRefToName(lVar,lRefGenotype) + " " + variantRefToName(cVar,cRefGenotype) + " " + corr);
                    }

                    // cVar must be after shardStart (check before this loop)
                    // if also before shardEnd then it is within shard
                    if (cVar.getStart() < shardEnd) {
                      c.output(variantRefToName(cVar,cRefGenotype) + " " + variantRefToName(lVar,lRefGenotype) + " " + corr);
                    }
                  }
                }
              }
            }
          }
        }

        // Store variants inside the Shard region to compare to variants read in later.
        if (cVar.getStart() < shardEnd) {
          vars.add(cVar);
        }
      }
    }
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(GenomicsDatasetOptions.class);
    GenomicsDatasetOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(GenomicsDatasetOptions.class);
    // Option validation is not yet automatic, we make an explicit call here.
    GenomicsDatasetOptions.Methods.validateOptions(options);

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
        .apply(ParDo.named("ComputeLD").of(new ProcessVariants(auth, 500L, 0.2)))

    // TODO: put into bitstore
        .apply(TextIO.Write.withoutSharding().to(options.getOutput()));

    p.run();
  }
}
