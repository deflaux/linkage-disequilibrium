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
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.common.primitives.Doubles;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;

/**
 * Computes linkage disequilibrium r between all variants inside the references list of a dataset to
 * all others within window. Outputs those pairs whose r is at least cutoff. For variants with
 * greater than two alleles ("Multiallelic" variants), each allele is compared to others and the
 * output name is prefixed with ":RefAllele".
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

  static class ComputeLdWorker extends DoFn<StreamVariantsRequest, String> {
    private final GenomicsFactory.OfflineAuth auth;
    private final long window;
    private final double cutoff;

    /**
     * Simplified variant with only information needed for computing and outputting pairwise LD
     * within a window
     */
    private class SimpleVariant {
      private final String id;
      private final String referenceName;
      private final long start;
      private final long end;
      private final int altCount;
      private final int[] genotypes;

      public SimpleVariant(String id, String referenceName, long start, long end, int altCount,
          int[] genotypes) {
        this.id = id;
        this.referenceName = referenceName;
        this.start = start;
        this.end = end;
        this.altCount = altCount;
        this.genotypes = genotypes;
      }

      public String getId() {
        return id;
      }

      public String getReferenceName() {
        return referenceName;
      }

      public long getStart() {
        return start;
      }

      public long getEnd() {
        return end;
      }

      public int getAltCount() {
        return altCount;
      }

      public int[] getGenotypes() {
        return genotypes;
      }
    }

    /**
     * Consumes shards and produces LD between all variants within a given shard and all variants
     * within window. Note: for pairs that are both found within a shard, LD is computed once and
     * output twice. For pairs between shards, LD is computed and outputted once per shard. This
     * allows for processing the data without having special logic for shards at boundaries
     */
    public ComputeLdWorker(GenomicsFactory.OfflineAuth auth, long window, double cutoff) {
      this.auth = auth;
      this.window = window;
      this.cutoff = cutoff;
    }

    class VariantProcessor {
      class CallSetGenotype {
        private final String id;
        private final int genotypeCount;

        public CallSetGenotype(String id, int genotypeCount) {
          this.id = id;
          this.genotypeCount = genotypeCount;
        }

        public String getId() {
          return id;
        }

        public int getGenotypeCount() {
          return genotypeCount;
        }
      }

      private final String referenceName;
      private final CallSetGenotype[] callSetGenotypes;
      private final int totalGenotypesCount;
      private long lastStart;

      public VariantProcessor(Variant var) {
        List<VariantCall> calls = var.getCallsList();
        int totalGenotypesCount = 0;
        CallSetGenotype[] callSetGenotypes = new CallSetGenotype[calls.size()];
        for (int i = 0; i < calls.size(); i++) {
          callSetGenotypes[i] =
              new CallSetGenotype(calls.get(i).getCallSetId(), calls.get(i).getGenotypeCount());
          totalGenotypesCount += callSetGenotypes[i].genotypeCount;
        }

        this.referenceName = var.getReferenceName();
        this.lastStart = var.getStart();
        this.totalGenotypesCount = totalGenotypesCount;
        this.callSetGenotypes = callSetGenotypes;
      }

      public SimpleVariant checkAndConvVariant(Variant var) {
        if (!referenceName.equals(var.getReferenceName())) {
          throw new IllegalArgumentException("Variant references do not match in shard.");
        }

        if (var.getStart() < lastStart) {
          throw new IllegalArgumentException("Variants in shard not sorted by start.");
        }

        lastStart = var.getStart();

        List<VariantCall> calls = var.getCallsList();

        if (callSetGenotypes.length != calls.size()) {
          throw new IllegalArgumentException("Number of variant calls do not match in shard.");
        }

        int[] genotypes = new int[totalGenotypesCount];

        for (int i = 0, j = 0; i < callSetGenotypes.length; i++) {
          VariantCall vc = calls.get(i);

          if (!callSetGenotypes[i].getId().equals(vc.getCallSetId())
              || callSetGenotypes[i].getGenotypeCount() != vc.getGenotypeCount()) {
            throw new IllegalArgumentException("Call sets do not match in shard.");
          }

          for (int k = 0; k < callSetGenotypes[i].getGenotypeCount(); k++, j++) {
            genotypes[j] = vc.getGenotype(k);

            if (genotypes[j] < -1 || genotypes[j] > var.getAlternateBasesCount()) {
              throw new IllegalArgumentException("Genotype outside allowable range.");
            }
          }
        }

        return new SimpleVariant(var.getId(), var.getReferenceName(), var.getStart(), var.getEnd(),
            var.getAlternateBasesCount(), genotypes);
      }
    }

    private double computeLd(int[] firstGenotypes, int firstRefGenotype, int[] secondGenotypes,
        int secondRefGenotype) {

      assert firstGenotypes.length == secondGenotypes.length;

      ArrayList<Double> firstValues = new ArrayList<Double>();
      ArrayList<Double> secondValues = new ArrayList<Double>();

      for (int i = 0; i < firstGenotypes.length; i++) {
        if (firstGenotypes[i] == -1 || secondGenotypes[i] == -1) {
          continue;
        }

        firstValues.add(firstGenotypes[i] == firstRefGenotype ? 0.0 : 1.0);
        secondValues.add(secondGenotypes[i] == secondRefGenotype ? 0.0 : 1.0);
      }

      return (new PearsonsCorrelation()).correlation(Doubles.toArray(firstValues),
          Doubles.toArray(secondValues));
    }

    private static String variantRefToName(SimpleVariant var, int refGenotype) {
      return var.getId() + ":" + var.getReferenceName() + ":" + var.getStart() + ":" + var.getEnd()
          + ((var.getAltCount() > 1) ? (":" + refGenotype) : "");
    }

    @Override
    public void processElement(ProcessContext c)
        throws java.io.IOException, java.security.GeneralSecurityException {

      VariantProcessor vp = null;

      /*
       * Our "working set" of variants that could overlap future variants. All these must be before
       * or overlapping the shardEnd. Should not contain anything that ends more than window from
       * the start of the previous variant.
       */
      LinkedList vars = new LinkedList<SimpleVariant>();

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

      SimpleVariant cVar = null;
      while (!varsToProcess.isEmpty() || streamIter.hasNext()) {
        if (varsToProcess.isEmpty()) {
          varsToProcess.addAll(streamIter.next().getVariantsList());
          continue;
        }

        if (vp == null) {
          vp = new VariantProcessor(varsToProcess.peek());
        }

        // Variant to compare to those in vars.
        cVar = vp.checkAndConvVariant(varsToProcess.remove());

        // Manually enforce "STRICT" overlaps here.
        if (cVar.getStart() >= shardStart) {
          ListIterator<SimpleVariant> varsIter = vars.listIterator(0);
          while (varsIter.hasNext()) {
            SimpleVariant lVar = varsIter.next();

            if (lVar.getEnd() + window <= cVar.getStart()) {
              varsIter.remove();
            } else {
              // If the number of alternative bases is > 1, then we do each allele vs. all others.
              for (int lRefGenotype = 0; lRefGenotype <= ((lVar.getAltCount() > 1)
                  ? lVar.getAltCount() : 0); lRefGenotype++) {
                for (int cRefGenotype = 0; cRefGenotype <= ((cVar.getAltCount() > 1)
                    ? cVar.getAltCount() : 0); cRefGenotype++) {

                  double corr = computeLd(lVar.getGenotypes(), lRefGenotype, cVar.getGenotypes(),
                      cRefGenotype);

                  // NOTE: NaN's are discarded (caused by no variation in one or more variant)
                  if (corr >= cutoff || corr <= -cutoff) {
                    // lVar must be before shardEnd (checked when adding to vars)
                    // if also after shardStart then it is within the shard
                    if (lVar.getStart() >= shardStart) {
                      c.output(variantRefToName(lVar, lRefGenotype) + " "
                          + variantRefToName(cVar, cRefGenotype) + " " + corr);
                    }

                    // cVar must be after shardStart (check before this loop)
                    // if also before shardEnd then it is within shard
                    if (cVar.getStart() < shardEnd) {
                      c.output(variantRefToName(cVar, cRefGenotype) + " "
                          + variantRefToName(lVar, lRefGenotype) + " " + corr);
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
            .of(new ComputeLdWorker(auth, options.getWindow(), options.getLdCutoff())))
        .apply(TextIO.Write.withoutSharding().to(options.getOutput()));

    p.run();
  }
}
