/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.genomics.dataflow.pipelines;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.model.LdValue;
import com.google.cloud.genomics.dataflow.utils.LdBigtableUtils;
import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This pipeline loads the results of the LinkageDisequilibrium pipeline into a Cloud BigTable.
 *
 * <p>
 * The Cloud BigTable must already be created and have a column family named "ld" to which all
 * results will be written. The key for the table enables fast searches by genomic region.
 *
 * <p>
 * This pipeline needs to be configured with four command line options for bigtable:
 * <ul>
 *   <li>--bigtableProjectId=[bigtable project]
 *   <li>--bigtableClusterId=[bigtable cluster id]
 *   <li>--bigtableZoneId=[bigtable zone]
 *   <li>--bigtableTableId=[bigtable tableName]
 * </ul>
 *
 * It also requires a glob path to Cloud Storage that identifies all input data to read.
 * <p>
 * Example call:
 * java -Xbootclasspath/p:lib/alpn-boot-8.1.6.v20151105.jar \
 *   -cp target/linkage-disequilibrium-*-runnable.jar \
 *   com.google.cloud.genomics.dataflow.pipelines.WriteLdBigtable \
 *   --runner=BlockingDataflowPipelineRunner \
 *   --project=YOUR_PROJECT_ID \
 *   --stagingLocation="gs://YOUR_BUCKET/staging" \
 *   --numWorkers=10 \
 *   --bigtableProjectId=YOUR_BIGTABLE_PROJECT_ID \
 *   --bigtableClusterId=YOUR_BIGTABLE_CLUSTER_ID \
 *   --bigtableZoneId=YOUR_BIGTABLE_ZONE \
 *   --bigtableTableId=YOUR_BIGTABLE_TABLE_ID \
 *   --ldInput="gs://PATH_TO_DIR_WITH_LD_DATA/*"
 */
public class WriteLdBigtable {

  /**
   * Options needed for running the WriteLdBigtable pipeline.
   *
   * <p>
   * In addition to standard Cloud BigTable options, this pipeline needs a specification of where
   * to read the input linkage disequilibrium data from.
   */
  public static interface WriteLdOptions extends CloudBigtableOptions {
    @Description("Path to input LD file(s) to load.")
    @Default.String("")
    String getLdInput();
    void setLdInput(String ldInput);
  }

  // A DoFn that reads a String LdValue and writes it to Cloud BigTable.
  static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {

    @Override
    public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
      LdValue entry = LdValue.fromLine(c.element());
      byte[] key = LdBigtableUtils.key(
          entry.getQuery().getReferenceName(),
          entry.getQuery().getStart(),
          entry.getQuery().getZeroAlleleBases(),
          entry.getQuery().getOneAlleleBases(),
          entry.getTarget().getReferenceName(),
          entry.getTarget().getStart(),
          entry.getTarget().getZeroAlleleBases(),
          entry.getTarget().getOneAlleleBases());

      c.output(
          new Put(key)
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QCHROM,
                  Bytes.toBytes(entry.getQuery().getReferenceName()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QSTART,
                  Bytes.toBytes(entry.getQuery().getStart()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QEND,
                  Bytes.toBytes(entry.getQuery().getEnd()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QID,
                  Bytes.toBytes(entry.getQuery().getCloudId()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QRSID,
                  Bytes.toBytes(entry.getQuery().getRsIds()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QNUMALT,
                  Bytes.toBytes(entry.getQuery().getAlternateBasesCount()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QZEROALLELE,
                  Bytes.toBytes(entry.getQuery().getZeroAlleleBases()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QONEALLELE,
                  Bytes.toBytes(entry.getQuery().getOneAlleleBases()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.TCHROM,
                  Bytes.toBytes(entry.getTarget().getReferenceName()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.TSTART,
                  Bytes.toBytes(entry.getTarget().getStart()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.TEND,
                  Bytes.toBytes(entry.getTarget().getEnd()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.TID,
                  Bytes.toBytes(entry.getTarget().getCloudId()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.TRSID,
                  Bytes.toBytes(entry.getTarget().getRsIds()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.TNUMALT,
                  Bytes.toBytes(entry.getTarget().getAlternateBasesCount()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.TZEROALLELE,
                  Bytes.toBytes(entry.getTarget().getZeroAlleleBases()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.TONEALLELE,
                  Bytes.toBytes(entry.getTarget().getOneAlleleBases()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.NCHROM,
                  Bytes.toBytes(entry.getCompCount()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.NQONES,
                  Bytes.toBytes(entry.getQueryOneAlleleCount()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.NTONES,
                  Bytes.toBytes(entry.getTargetOneAlleleCount()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.NBONES,
                  Bytes.toBytes(entry.getQueryAndTargetOneAlleleCount()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.CORR,
                  Bytes.toBytes(entry.getR()))
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.DPRIME,
                  Bytes.toBytes(entry.getDPrime())));
    }
  };

  /**
   * <p>Creates a dataflow pipeline that creates the following chain:</p>
   * <ol>
   *   <li> Reads LdValue lines from Google Cloud Storage into the Pipeline
   *   <li> Creates Puts from each of the LdValue lines
   *   <li> Inserts the Puts into an existing Cloud BigTable
   * </ol>
   *
   * @param args Arguments to use to configure the Dataflow Pipeline. See example call above.
   */
  public static void main(String[] args) {
    WriteLdOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteLdOptions.class);
    Preconditions.checkArgument(!options.getLdInput().isEmpty(), "--ldInput must be specified.");

    // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
    CloudBigtableTableConfiguration config =
        CloudBigtableTableConfiguration.fromCBTOptions(options);

    Pipeline pipeline = Pipeline.create(options);
    // This sets up serialization for Puts and Deletes so that Dataflow can move them through the
    // network.
    CloudBigtableIO.initializeForWrite(pipeline);

    pipeline
       .apply(TextIO.Read.named("linkage_disequilibrium_data")
                         .from(options.getLdInput()))
       .apply(ParDo.of(MUTATION_TRANSFORM))
       .apply(CloudBigtableIO.writeToTable(config));

    pipeline.run();

  }
}
