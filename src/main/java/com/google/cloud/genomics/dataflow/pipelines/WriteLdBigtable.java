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
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.model.LdResult;
import com.google.cloud.genomics.dataflow.utils.LdBigtableUtils;

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
 *   -cp target/linkage-disequilibrium-v1-0.1-SNAPSHOT-runnable.jar \
 *   com.google.cloud.genomics.dataflow.pipelines.WriteLdBigtable \
 *   --runner=BlockingDataflowPipelineRunner \
 *   --project=<YOUR_PROJECT_ID> \
 *   --stagingLocation="gs://<YOUR_BUCKET>/staging" \
 *   --numWorkers=10 \
 *   --bigtableProjectId=<YOUR_BIGTABLE_PROJECT_ID> \
 *   --bigtableClusterId=<YOUR_BIGTABLE_CLUSTER_ID> \
 *   --bigtableZoneId=<YOUR_BIGTABLE_ZONE> \
 *   --bigtableTableId=<YOUR_BIGTABLE_TABLE_ID> \
 *   --ldInput="gs://<PATH_TO_DIR_WITH_LD_DATA>/*"
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
    String getLdInput();
    void setLdInput(String ldInput);
  }

  // A DoFn that reads a String LdResult and writes it to Cloud BigTable.
  static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {

    @Override
    public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
      LdResult entry = LdResult.fromLine(c.element());
      byte[] key = LdBigtableUtils.key(
          entry.queryChrom(),
          entry.queryStart(),
          entry.queryZeroAllele(),
          entry.queryOneAllele(),
          entry.targetChrom(),
          entry.targetStart(),
          entry.targetZeroAllele(),
          entry.targetOneAllele());

      c.output(
          new Put(key)
              .addColumn(
                  LdBigtableUtils.FAMILY,
                  LdBigtableUtils.QUALIFIER,
                  Bytes.toBytes(c.element())));
    }
  };

  /**
   * <p>Creates a dataflow pipeline that creates the following chain:</p>
   * <ol>
   *   <li> Reads LdResult lines from Google Cloud Storage into the Pipeline
   *   <li> Creates Puts from each of the LdResult lines
   *   <li> Inserts the Puts into an existing Cloud BigTable
   * </ol>
   *
   * @param args Arguments to use to configure the Dataflow Pipeline. See example call above.
   */
  public static void main(String[] args) {
    WriteLdOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteLdOptions.class);

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
