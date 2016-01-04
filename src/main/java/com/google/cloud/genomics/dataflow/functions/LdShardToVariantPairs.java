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
package com.google.cloud.genomics.dataflow.functions;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.model.LdVariant;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Part of the LinkageDisequilibrium pipeline that consumes pairs of LdVariant lists (one query
 * and one target), and emits all pairs of LdVariants to cover all pairs of comparisons where 
 * the LdVariant from the query is "before" (as defined by LdVariant.compareTo()) the target 
 * LdVariant and the end of query is within window of the start of target.
 */
public class LdShardToVariantPairs
    extends DoFn<KV<String, Iterable<KV<Boolean, List<LdVariant>>>>, KV<LdVariant, LdVariant>> {
  private final long window;

  /** 
   * Parameter used to create all legal pairs of comparisons.
   *
   * @param window Maximum window between variants.
   */
  public LdShardToVariantPairs(long window) {
    this.window = window;
  }

  /**
   * Processes LdVariant lists to produce pairs of LdVariants. 
   *
   * All the pairs that are emitted here must be desired comparisons with no duplicates -- 
   * no additional filters are done other than (optionally) on the LD value itself. 
   */
  @Override
  public void processElement(ProcessContext c) {
    List<LdVariant> queryList = null;
    List<LdVariant> targetList = null;
    int inputListCount = 0;
    for (KV<Boolean, List<LdVariant>> vl : c.element().getValue()) {
      if (vl.getKey()) {
        queryList = vl.getValue();
      } else {
        targetList = vl.getValue();
      }
      inputListCount++;
    }

    if (inputListCount != 2 || queryList == null || targetList == null) {
      throw new IllegalArgumentException("There should be exactly two lists.");
    }

    Iterator<LdVariant> targetIter = targetList.iterator();

    // Our working set of variants from the target shard that could be within window of variants
    // read in from the query shard.
    LinkedList<LdVariant> storedTarget = new LinkedList<>();

    for (LdVariant queryVar : queryList) {
      // Fill in storedTarget until we are past window from queryVar.
      while (targetIter.hasNext()
          && (storedTarget.isEmpty() || (queryVar.getInfo().getEnd() + window) > storedTarget
              .getLast().getInfo().getStart())) {
        storedTarget.add(targetIter.next());
      }

      ListIterator<LdVariant> storedTargetIter = storedTarget.listIterator(0);
      while (storedTargetIter.hasNext()) {
        LdVariant targetVar = storedTargetIter.next();

        if (queryVar.getInfo().compareTo(targetVar.getInfo()) != -1) {
          // Only do comparisons where query is before target. If query is past target then
          // we are done with target. Note: targetVar > queryVar implies targetVar start >= queryVar
          // start.
          storedTargetIter.remove();
        } else if (targetVar.getInfo().getStart() >= (queryVar.getInfo().getEnd() + window)) {
          // If the next query starts more than window from when target starts, we are done
          // with the rest of storedTarget for this query.
          break;
        } else {
          // If target starts >= query starts and target starts < window from query end, then they
          // must overlap.
          c.output(KV.of(queryVar, targetVar));
        }
      }
    }
  }
}

