# linkage-disequilibrium
[![Build Status](https://img.shields.io/travis/googlegenomics/linkage-disequilibrium.svg?style=flat)](https://travis-ci.org/googlegenomics/linkage-disequilibrium) [![Coverage Status](https://img.shields.io/coveralls/googlegenomics/linkage-disequilibrium.svg?style=flat)](https://coveralls.io/r/googlegenomics/linkage-disequilibrium)

This repository contains tools for generating and interacting with
[linkage disequilibrium](https://en.wikipedia.org/wiki/Linkage_disequilibrium)
(LD) data, including [Google Cloud Dataflow](https://cloud.google.com/dataflow/)
pipelines to calculate LD from a
[Google Genomics](https://cloud.google.com/genomics/) variant set and write the
results to [Google Cloud Storage](https://cloud.google.com/storage/), load a set
of LD results from Cloud Storage into a
[Cloud BigTable](https://cloud.google.com/bigtable/docs/), and efficiently query
a Cloud BigTable for LD data.

## Background
LD is the non-random association of alleles at distinct physical loci, and can
arise due to many different factors in population genetics.

Knowing the extent of LD between variants is essential for filtering when using
various population genetics algorithms, such as using principal component
analysis to identify genome-wide population structure. Furthermore, LD between
variants can be exploited when one is interested in an individual’s genotype at
a particular variant of interest, but that variant is not directly assayed in
the experiment used (i.e. as a quick proxy for genotype imputation).

Calculating LD between a single pair of variants is conceptually simple.
However, it is computationally burdensome for large data sets, as the number of
calculations grows as O(N^2) for a data set of N variants.

This repository demonstrates the ability to perform the resource-heavy LD
computation efficiently using Dataflow, as well as ways to interact with the
resulting data.

## Getting started

1. git clone this repository.

1. If you have not already done so, follow the Google Genomics
   [getting started instructions](https://cloud.google.com/genomics/install-genomics-tools)
   to set up your environment including
   [installing gcloud](https://cloud.google.com/sdk/) and running `gcloud init`.

1. If you have not already done so, follow the Dataflow
   [getting started instructions](https://cloud.google.com/dataflow/getting-started)
   to set up your environment for Dataflow.

1. Download the correct version of the
   [ALPN documentation](http://www.eclipse.org/jetty/documentation/9.2.10.v20150310/alpn-chapter.html).

  2. See the
     [ALPN documentation](http://www.eclipse.org/jetty/documentation/9.2.10.v20150310/alpn-chapter.html)
     for a table of which ALPN jar to use for your JRE version.
  2. Then download the correct version from
     [here](http://mvnrepository.com/artifact/org.mortbay.jetty.alpn/alpn-boot).

1. Use a recent version of [Apache Maven](http://maven.apache.org/download.cgi)
   (e.g., version 3.3.3) to build this code:
```
cd linkage-disequilibrium
mvn package
```

Now, pipelines can be run on Google Compute Engine using the example commands
listed within the pipeline files themselves (see, for example,
[WriteLdBigtable.java](src/main/java/com/google/cloud/genomics/dataflow/pipelines/WriteLdBigtable.java)).

## Linkage disequilibrium calculation pipeline
Because calculation of LD can be performed independently for all pairs of
variants of interest, it is an ideal candidate for parallelization. The
`LinkageDisequilibrium.java` pipeline takes as input a Google Genomics variant
set and writes LD output to Cloud Storage as a file of LD results.

Each LD result is represented as a comma-separated line with 22 entries. Each
variant is represented by eight values:

1. chromosome
1. start position (0-based half-open, like
   [UCSC BED format](https://genome.ucsc.edu/FAQ/FAQformat.html#format1))
1. end position (0-based half open, like UCSC BED format)
1. Google Genomics variant ID
1. Semicolon-delimited list of rsids assigned to the variant
1. The number of non-reference alleles defined for the variant
1. The allele coded as 0 in the LD calculations
1. The allele coded as 1 in the LD calculations

The final six values are:

17. The number of chromosomes used in the LD calculation
17. The number of chromosomes for which the query variant has the 1 allele
17. The number of chromosomes for which the target variant has the 1 allele
17. The number of chromosomes for which both the query and target variants have the 1 allele
17. The
    [allelic correlation coefficient measure](https://en.wikipedia.org/wiki/Linkage_disequilibrium#Measures_of_linkage_disequilibrium_derived_from)
    of LD between the variants
17. The
    [D' measure](https://en.wikipedia.org/wiki/Linkage_disequilibrium#Measures_of_linkage_disequilibrium_derived_from)
    of LD between the variants

### Design decisions in LD calculations
There are multiple design decisions made in the `LinkageDisequilibrium.java`
pipeline that influence its results. Ensure your data is appropriate before
running the pipeline blindly.

#### LD measures
The LD measures chosen to calculate were allelic correlation coefficient and
D'. Note that each of these expects
[phased data](https://en.wikipedia.org/wiki/Haplotype_estimation) as input.

#### Missing data
The standard way to handle missing data is to calculate the correlation based
only on individuals who have genotype calls at both variants of interest.
This may be problematic if no-calls are correlated with the presence of a
particular allele, but is an expected way to handle the missing data. This is
the method used in the pipeline.

A comparison to two other strategies (imputation or sampling) supports this
being the method that most accurately recovers the true underlying Pearson
correlation.

#### Multiallelic sites
Genotypic correlation is defined in the context of two alleles, and simply
calculates the correlation between the counts of a given allele at each
variant. Genotypic correlation is ill-defined in the context of variants
with more than two distinct alleles.

Frequently, tri- or higher-allelic variants have two alleles that are present
in nearly all individuals, with the other variants being extremely rare. As a
result, we handle multiallelic sites by identifying the two most frequent
alleles, and only using chromosomes that contain one of those two alleles in
the LD calculation (i.e., effectively treating rare alleles as no-calls).

#### Sex chromosomes
Different models can be used to model LD on sex chromosomes. This pipeline
uses the same genotype counts as encoded in the Variant in its calculation
of LD metrics.

## Loading LD calculations into BigQuery
Because the LD calculation pipeline writes its output as comma-separated lines,
it is easy to load the data into [BigQuery](https://cloud.google.com/bigquery/).

General instructions for loading data into BigQuery are available
[here](https://cloud.google.com/bigquery/loading-data-into-bigquery). An example
script for loading from CSV is available at
[load_data_from_csv.py](https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/bigquery/api/load_data_from_csv.py).
When using that script, the
[schema](https://cloud.google.com/bigquery/docs/reference/v2/tables) for the
table of LD results is available in this repository in the
[ld_bigquery_schema_fields.txt](schema/ld_bigquery_schema_fields.txt) file.

Consequently, LD data can be loaded into a BigQuery table with the following
code snippet:

```
PROJECTID=<your-project-id>
DATASETID=<your-bigquery-dataset-id>
TABLE=<your-desired-bigquery-table-name>
DATA=<path-to-linkage-disequilibrium-result-data>

python path/to/load_data_from_csv.py \
  $PROJECTID $DATASETID $TABLE schema/ld_bigquery_schema_fields.txt $DATA
```

## Loading LD calculations into BigTable
Because BigTable allows efficient access to extremely large datasets indexed by
a single key, it is a natural choice for representation of LD data. The pipeline
`WriteLdBigtable.java` provides an example in which Dataflow is used to read
data generated by the `LinkageDisequilibrium.java` pipeline and writes the
results into a BigTable. The key for each BigTable row is designed so that all
LD results for a single query variant appear in a contiguous block of the table,
sorted by the location of the target variants, and results for query variants
are sorted by the location of query variants. This key design allows efficient
access to all LD results for a single variant or a single region of the genome.

An example command to run the pipeline is given within the
[WriteLdBigtable.java](src/main/java/com/google/cloud/genomics/dataflow/pipelines/WriteLdBigtable.java)
source code itself.

## Querying LD calculations stored in BigTable
Once a BigTable storing LD data has been created, a mechanism for accessing the
results must be created. The
[QueryLdBigtable.java](src/main/java/com/google/cloud/genomics/dataflow/pipelines/QueryLdBigtable.java)
pipeline provides an example in which Dataflow is used to read a subset of data
from an LD BigTable and write the results to GCS in the same format as it was
originally written by the
[LinkageDisequilibrium.java](src/main/java/com/google/cloud/genomics/dataflow/pipelines/LinkageDisequilibrium.java)
pipeline.

An example command to run the pipeline is given within the
[QueryLdBigtable.java](src/main/java/com/google/cloud/genomics/dataflow/pipelines/QueryLdBigtable.java)
source code itself.

## Publicly-available LD datasets
Coming soon...

## Troubleshooting

1. If the
   [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials)
   are not sufficient to run the `LinkageDisequilibrium.java` pipeline, add the
   `--client-secrets PATH/TO/YOUR/client_secrets.json` flag to the command. If
   you do not already have this file, see the
   [authentication instructions](https://cloud.google.com/genomics/install-genomics-tools#authenticate)
   to obtain it.
