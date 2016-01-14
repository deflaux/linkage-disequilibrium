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
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.model.LdValue;
import com.google.cloud.genomics.dataflow.utils.LdBigtableUtils;
import com.google.common.base.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This pipeline reads linkage disequilibrium results from a Cloud BigTable.
 *
 * <p>
 * The Cloud BigTable from which LD results are read must have been populated by the
 * WriteLdBigtable pipeline.
 *
 * <p>
 * This pipeline needs to be configured with four command line options for bigtable:
 * <ul>
 *  <li> --bigtableProjectId=[bigtable project]
 *  <li> --bigtableClusterId=[bigtable cluster id]
 *  <li> --bigtableZoneId=[bigtable zone]
 *  <li> --bigtableTableId=[bigtable tableName]
 * </ul>
 *
 * It also requires two additional flags:
 * <ul>
 *   <li> --queryRange=[chrom:start-end]
 *   <li> --resultLocation=[path to output storage location]
 * </ul>
 * <p>
 * Example call:
 * java -Xbootclasspath/p:lib/alpn-boot-8.1.6.v20151105.jar \
 *   -cp target/linkage-disequilibrium-*-runnable.jar \
 *   com.google.cloud.genomics.dataflow.pipelines.QueryLdBigtable \
 *   --runner=BlockingDataflowPipelineRunner \
 *   --project=YOUR_PROJECT_ID \
 *   --stagingLocation="gs://YOUR_BUCKET/staging" \
 *   --numWorkers=10 \
 *   --bigtableProjectId=YOUR_BIGTABLE_PROJECT_ID \
 *   --bigtableClusterId=YOUR_BIGTABLE_CLUSTER_ID \
 *   --bigtableZoneId=YOUR_BIGTABLE_ZONE \
 *   --bigtableTableId=YOUR_BIGTABLE_TABLE_ID \
 *   --queryRange="22:10-50000000" \
 *   --resultLocation="gs://PATH_TO_OUTPUT_FILE"
 */
public class QueryLdBigtable {

  // Regular expression to match valid query variant ranges.
  private static final Pattern RANGE_PATTERN =
      Pattern.compile("^(?<chrom>\\d\\d?)(:(?<start>\\d+)(-(?<end>\\d+))?)?$");

  /**
   * Options needed for running the QueryLdBigtable pipeline.
   * <p>
   * In addition to standard Cloud BigTable options, this pipeline needs a specification of
   * the query range and where to write the result data.
   */
  public static interface QueryLdOptions extends CloudBigtableOptions {
    // The input query range, formatted as one of the following 3 options:
    //   <chrom>                // Returns results for the entire chromosome
    //   <chrom>:<start>        // Returns results for queries starting at this position
    //   <chrom>:<start>-<end>  // Returns results for all queries with start position in this range
    @Description("The query range to search, e.g. chrom:start-end")
    String getQueryRange();
    void setQueryRange(String queryRange);

    @Description("The cloud bucket file where (sharded) results are written.")
    String getResultLocation();
    void setResultLocation(String resultLocation);
  }

  // Converts a BigTable Result to a String so that it can be written to a file.
  static final DoFn<Result, String> STRINGIFY = new DoFn<Result, String>() {
    @Override
    public void processElement(DoFn<Result, String>.ProcessContext ctx) throws Exception {
      LdValue value = LdValue.fromTokens(
          Bytes.toString(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.QCHROM)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.QSTART)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.QEND)),
          Bytes.toString(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.QID)),
          Bytes.toString(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.QRSID)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.QNUMALT)),
          Bytes.toString(
              ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.QZEROALLELE)),
          Bytes.toString(
              ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.QONEALLELE)),
          Bytes.toString(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.TCHROM)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.TSTART)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.TEND)),
          Bytes.toString(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.TID)),
          Bytes.toString(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.TRSID)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.TNUMALT)),
          Bytes.toString(
              ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.TZEROALLELE)),
          Bytes.toString(
              ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.TONEALLELE)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.NCHROM)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.NQONES)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.NTONES)),
          Bytes.toInt(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.NBONES)),
          Bytes.toDouble(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.CORR)),
          Bytes.toDouble(ctx.element().getValue(LdBigtableUtils.FAMILY, LdBigtableUtils.DPRIME)));
      ctx.output(value.toString());
    }
  };

  /**
   * <p>Creates a dataflow pipeline that creates the following chain:</p>
   * <ol>
   *   <li> Reads a region of the requested Ld Cloud BigTable
   *   <li> Transforms the rows of the result into string values
   *   <li> Writes the results to a Cloud Storage bucket
   * </ol>
   *
   * @param args Arguments to use to configure the Dataflow Pipeline. See example call above.
   */
  public static void main(String[] args) {
    QueryLdOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(QueryLdOptions.class);

    // Create a scan based on the requested query range.
    Scan scan = queryScan(options.getQueryRange());
    scan.setCacheBlocks(false);

    // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
    // Rows are filtered using the query range scan defined above.
    CloudBigtableScanConfiguration config =
        CloudBigtableScanConfiguration.fromCBTOptions(options, scan);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
       .apply(Read.from(CloudBigtableIO.read(config)))
       .apply(ParDo.of(STRINGIFY))
       .apply(TextIO.Write.withoutSharding().to(options.getResultLocation()));

    pipeline.run();
    // Once this is done, you can get the result file via "gsutil cp <resultLocation>* ."
  }

  /**
   * Returns a Scan object that enforces the specified queryRange.
   */
  private static Scan queryScan(String queryRange) {
    Matcher m = RANGE_PATTERN.matcher(queryRange);
    Preconditions.checkArgument(m.matches(), "Invalid query range: %s", queryRange);
    String chrom = m.group("chrom");
    String startStr = m.group("start");
    String endStr = m.group("end");
    byte[] startRow;
    byte[] endRow;
    if (startStr == null) {
      // This is matching the entire chromosome
      startRow = LdBigtableUtils.keyStart(chrom);
      endRow = LdBigtableUtils.keyEnd(chrom);
    } else if (endStr == null) {
      // Start but not end was specified, this should match the start
      int position = Integer.valueOf(startStr);
      startRow = LdBigtableUtils.keyStart(chrom, position);
      endRow = LdBigtableUtils.keyEnd(chrom, position);
    } else {
      int start = Integer.valueOf(startStr);
      int end = Integer.valueOf(endStr);
      startRow = LdBigtableUtils.keyStart(chrom, start);
      endRow = LdBigtableUtils.keyEnd(chrom, end);
    }
    return new Scan(startRow, endRow);
  }
}
