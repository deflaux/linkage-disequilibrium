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
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Wrapper for VariantStreamIterator.
 *
 * <ul>
 * <li>Ensures that variants come from the same reference in sorted order (as indicated by
 * LdVariantInfo.compareTo). Note: input variants from VariantStreamIterator are sorted by start
 * only.
 * <li>Rather than return a list of variants per next() call, returns one at a time.
 * <li>Discards LdVariants that do not have variation (and thus have undefined LD to any other
 * variant.
 * </ul>
 */
public class LdVariantStreamIterator implements Iterator<LdVariant> {
  private Iterator<StreamVariantsResponse> streamIter;
  private PriorityQueue<LdVariant> storedLdVars =
      new PriorityQueue<>(11, new LdVariantComparator());
  private LdVariantProcessor ldVariantProcessor = null;
  private LdVariant nextLdVariant = null;
  private long lastStart = -1;
  private final String referenceName;

  /**
   * Create new LdVariantStreamIterator.
   *
   * @param request Stream of variants to read from.
   * @param auth Authorization for reading variants.
   * @param ldVariantProcessor LdVariantProcessor to run each variant through.
   */
  public LdVariantStreamIterator(StreamVariantsRequest request, OfflineAuth auth,
      LdVariantProcessor ldVariantProcessor)
          throws java.io.IOException, java.security.GeneralSecurityException {
    streamIter = VariantStreamIterator.enforceShardBoundary(auth, request,
        ShardBoundary.Requirement.OVERLAPS, null);
    referenceName = request.getReferenceName();
    this.ldVariantProcessor = ldVariantProcessor;
  }

  private class LdVariantComparator implements Comparator<LdVariant> {
    @Override
    public int compare(LdVariant x, LdVariant y) {
      return x.getInfo().compareTo(y.getInfo());
    }
  }

  /**
   * Returns true when there are remaining varints in the stream with Variation.
   */
  public boolean hasNext() {
    while (storedLdVars.isEmpty()
        || (storedLdVars.peek().getInfo().getStart() == lastStart && streamIter.hasNext())) {
      if (!streamIter.hasNext()) {
        return false;
      }

      for (Variant v : streamIter.next().getVariantsList()) {
        LdVariant lv = ldVariantProcessor.convertVariant(v);

        if (!referenceName.equals(lv.getInfo().getReferenceName())
            || lastStart > lv.getInfo().getStart()) {
          throw new IllegalArgumentException("Variants are not streamed in increasing order.");
        }

        lastStart = lv.getInfo().getStart();

        if (lv.hasVariation()) {
          storedLdVars.add(lv);
        }
      }
    }

    return true;
  }

  public LdVariant next() {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }

    return storedLdVars.poll();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}

