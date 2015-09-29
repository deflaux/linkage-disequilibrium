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
package com.google.cloud.genomics.dataflow.model;

import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.genomics.v1.StreamVariantsRequest;

import java.io.Serializable;

/**
 * Container to store a pair of StreamVariantsRequest that are meant to be compared to each other
 * along with the ShardBoundary for each request.
 */
public class LdCalculatorPairRequest implements Serializable {
  private final StreamVariantsRequest query;
  private final ShardBoundary.Requirement queryBoundary;
  private final StreamVariantsRequest target;
  private final ShardBoundary.Requirement targetBoundary;

  public LdCalculatorPairRequest(StreamVariantsRequest query,
      ShardBoundary.Requirement queryBoundary, StreamVariantsRequest target,
      ShardBoundary.Requirement targetBoundary) {
    this.query = query;
    this.queryBoundary = queryBoundary;
    this.target = target;
    this.targetBoundary = targetBoundary;
  }

  public StreamVariantsRequest getQuery() {
    return query;
  }

  public ShardBoundary.Requirement getQueryBoundary() {
    return queryBoundary;
  }

  public StreamVariantsRequest getTarget() {
    return target;
  }

  public ShardBoundary.Requirement getTargetBoundary() {
    return targetBoundary;
  }
}

