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
package com.google.cloud.genomics.dataflow.utils;

import com.google.cloud.genomics.dataflow.model.LdVariant;
import com.google.cloud.genomics.dataflow.utils.LdVariantProcessor;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * Wrapper for VariantStreamIterator.
 *
 * 1. Checks that variants come from the same reference and in non-decreasing order of start.
 *
 * 2. Rather than return a list of variants per next() call, returns one at a time.
 *
 * 3. Discards LdVariants that do not have variation.
 */
public class LdVariantStreamIterator implements Iterator<LdVariant> {
  private Iterator<StreamVariantsResponse> streamIter;
  private Queue<Variant> varsToProcess = new LinkedList<>();
  private LdVariantProcessor ldVaraintProcessor = null;
  private LdVariant nextLdVariant = null;
  private long lastStart = -1;
  private final String referenceName;

  public LdVariantStreamIterator(StreamVariantsRequest request, GenomicsFactory.OfflineAuth auth,
      ShardBoundary.Requirement shardBoundaryRequirement)
          throws java.io.IOException, java.security.GeneralSecurityException {
    streamIter = new VariantStreamIterator(request, auth, shardBoundaryRequirement, null);
    referenceName = request.getReferenceName();
  }

  public LdVariantStreamIterator(StreamVariantsRequest request, GenomicsFactory.OfflineAuth auth,
      ShardBoundary.Requirement shardBoundaryRequirement, LdVariantStreamIterator ldVariantIterator)
          throws java.io.IOException, java.security.GeneralSecurityException {
    this(request, auth, shardBoundaryRequirement);
    this.ldVaraintProcessor = ldVariantIterator.ldVaraintProcessor;
  }

  public boolean hasNext() {
    while (nextLdVariant == null) {
      while (varsToProcess.isEmpty()) {
        if (!streamIter.hasNext()) {
          return false;
        }

        varsToProcess.addAll(streamIter.next().getVariantsList());
      }

      if (ldVaraintProcessor == null) {
        ldVaraintProcessor = new LdVariantProcessor(varsToProcess.peek());
      }

      nextLdVariant = ldVaraintProcessor.convertVariant(varsToProcess.remove());

      if (!referenceName.equals(nextLdVariant.getInfo().getReferenceName())
          || lastStart > nextLdVariant.getInfo().getStart()) {
        throw new IllegalArgumentException("Variants are not streamed in increasing order.");
      }

      lastStart = nextLdVariant.getInfo().getStart();

      if (!nextLdVariant.hasVariation()) {
        nextLdVariant = null;
      }
    }

    return true;
  }

  public LdVariant next() {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }

    LdVariant v = nextLdVariant;
    nextLdVariant = null;

    return v;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}

