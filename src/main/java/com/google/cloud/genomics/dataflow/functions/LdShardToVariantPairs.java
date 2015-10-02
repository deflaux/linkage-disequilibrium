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
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.model.LdVariant;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 */
public class LdShardToVariantPairs extends
    DoFn<KV<KV<KV<String, KV<Long, Long>>, KV<Integer, Integer>>, CoGbkResult>, KV<LdVariant, LdVariant>> {
  private final long window;
  private final int shardsPerWindow;
  private final TupleTag<List<LdVariant>> queryListTupleTag;
  private final TupleTag<List<LdVariant>> targetListTupleTag;

  public LdShardToVariantPairs(long window, int shardsPerWindow,
      TupleTag<List<LdVariant>> queryListTupleTag, TupleTag<List<LdVariant>> targetListTupleTag) {
    this.window = window;
    this.shardsPerWindow = shardsPerWindow;
    this.queryListTupleTag = queryListTupleTag;
    this.targetListTupleTag = targetListTupleTag;
  }

  @Override
  public void processElement(ProcessContext c) {
    KV<KV<KV<String, KV<Long, Long>>, KV<Integer, Integer>>, CoGbkResult> e = c.element();

    if (!e.getValue().getAll(queryListTupleTag).iterator().hasNext()
        || !e.getValue().getAll(targetListTupleTag).iterator().hasNext()) {
      return;
    }

    List<LdVariant> queryList = c.element().getValue().getAll(queryListTupleTag).iterator().next();
    List<LdVariant> targetList =
        c.element().getValue().getAll(targetListTupleTag).iterator().next();

    long contigStart = e.getKey().getKey().getValue().getKey();
    int queryShardIndex = e.getKey().getValue().getKey();
    int targetShardIndex = e.getKey().getValue().getValue();

    // -1 means no filter
    long queryStartFilter = -1;
    long targetStartFilter = contigStart + targetShardIndex * window;

    if (queryShardIndex != 0 && (targetShardIndex - queryShardIndex) < shardsPerWindow) {
      queryStartFilter = contigStart + queryShardIndex * window;
    }

    Iterator<LdVariant> targetIter = targetList.iterator();

    // Our working set of variants from the target shard that could be within window of variants
    // read in from the query shard.
    LinkedList<LdVariant> storedVars = new LinkedList<>();

    for (LdVariant queryVar : queryList) {
      if (queryVar.getInfo().getStart() < queryStartFilter) {
        continue;
      }

      // Fill in storedVars until we are past window from queryVar.
      while (targetIter.hasNext() && (storedVars.isEmpty()
          || (queryVar.getInfo().getEnd() + window) > storedVars.getLast().getInfo().getStart())) {
        LdVariant targetVar = targetIter.next();

        if (targetVar.getInfo().getStart() >= targetStartFilter) {
          storedVars.add(targetIter.next());
        }
      }

      // Remove variants in storedVars that we are more than window away from and output overlaps.
      ListIterator<LdVariant> varsIter = storedVars.listIterator(0);
      while (varsIter.hasNext()) {
        LdVariant targetVar = varsIter.next();

        // The StreamVariantsResponse Iterator does not have an obvious total ordering (although
        // it does sort by referenceName and start). Thus, there are variants that we may have
        // passed (according to LdVariantInfo.compareTo), but have the same start and we should
        // not remove because the next targetVar may be "before" it.
        //
        // If self-comparisons are desired (e.g. to have a list of the variants with variation)
        // then the compareTo check should be != 1 and the c.output on cr.reverse should only
        // be done if they are not the same variant.
        if (queryVar.getInfo().getStart() > targetVar.getInfo().getStart()) {
          varsIter.remove();
        } else if (queryVar.getInfo().compareTo(targetVar.getInfo()) == -1
            && (queryVar.getInfo().getEnd() + window) > targetVar.getInfo().getStart()) {

          c.output(KV.of(queryVar, targetVar));
        }
      }
    }
  }
}

