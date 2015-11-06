/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.genomics.dataflow.utils;

import org.junit.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Before;
import org.junit.Rule;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;
import com.google.cloud.genomics.dataflow.model.LdVariant;
import org.junit.rules.ExpectedException;

@RunWith(JUnit4.class)
public class LdVariantProcessorTest {

  private Variant v1, v2, v3;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    VariantCall[][] calls = new VariantCall[2][2];

    for (int i=0; i<2; i++) {
      for (int j=0; j<2; j++) {
        calls[i][j] = VariantCall.newBuilder().setCallSetName(String.valueOf(i*10+j)).setPhaseset("*").addGenotype(i).addGenotype(j).build();
      }
    }

    v1 = Variant.newBuilder()
        .setReferenceName("chr3").setStart(10000).setEnd(10001).setReferenceBases("A").addAlternateBases("T")
        .addAllCalls(Arrays.asList(calls[0][0], calls[0][1], calls[0][1])).build();

    v2 = Variant.newBuilder()
        .setReferenceName("chr3").setStart(10001).setEnd(10002).setReferenceBases("A").addAlternateBases("T")
        .addAllCalls(Arrays.asList(calls[0][0], calls[0][1], calls[0][1])).build();

    v3 = Variant.newBuilder()
        .setReferenceName("chr3").setStart(10001).setEnd(10002).setReferenceBases("A").addAlternateBases("T")
        .addAllCalls(Arrays.asList(calls[0][0], calls[0][1], calls[0][1], calls[0][1])).build();
  }

  @Test
  public void testLdVariantProcessor() {
    LdVariantProcessor vp = new LdVariantProcessor(Arrays.asList("0", "1", "1"));

    LdVariant lv1 = vp.convertVariant(v1);
    LdVariant lv2 = vp.convertVariant(v2);

    Assert.assertTrue(lv1.getInfo().compareTo(lv2.getInfo()) == -1);
    Assert.assertFalse(lv1.getInfo().equals(lv2.getInfo()));
    Assert.assertTrue(lv1.getInfo().equals(lv1.getInfo()));

    Assert.assertTrue(v1.getReferenceName().equals("chr3"));
  }

  @Test
  public void testCallSetSizeMismatch() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Mismatch in number of calls.");
    LdVariantProcessor vp = new LdVariantProcessor(Arrays.asList("", ""));
    vp.convertVariant(v3);
  }

  @Test
  public void testCallSetNameMismatch() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("CallSetName mismatch: A vs 0");
    LdVariantProcessor vp = new LdVariantProcessor(Arrays.asList("A", "B", "C", "D"));
    vp.convertVariant(v3);
  }
}
