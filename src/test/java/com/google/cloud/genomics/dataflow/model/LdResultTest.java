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
package com.google.cloud.genomics.dataflow.model;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the LdResult class.
 */
@RunWith(JUnit4.class)
public class LdResultTest {

  // An example valid LdResult string.
  private static final String STRVAL = String.format(
      "%s,%s",
      "22,49800071,49800072,12935662044341619522,rs9628002,1,A,G,22,49817276,49817277",
      "14796642673562247416,rsXXX,2,C,CA,1006,516,475,456,0.434039,0.664331");

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void loadFromString() {
    LdResult result = LdResult.fromLine(STRVAL);
    assertEquals(result.queryChrom(), "22");
    assertEquals(result.queryStart(), 49800071);
    assertEquals(result.queryEnd(), 49800072);
    assertEquals(result.queryCloudId(), "12935662044341619522");
    assertEquals(result.queryNames(), "rs9628002");
    assertEquals(result.queryNumAltAlleles(), 1);
    assertEquals(result.queryZeroAllele(), "A");
    assertEquals(result.queryOneAllele(), "G");

    assertEquals(result.targetChrom(), "22");
    assertEquals(result.targetStart(), 49817276);
    assertEquals(result.targetEnd(), 49817277);
    assertEquals(result.targetCloudId(), "14796642673562247416");
    assertEquals(result.targetNames(), "rsXXX");
    assertEquals(result.targetNumAltAlleles(), 2);
    assertEquals(result.targetZeroAllele(), "C");
    assertEquals(result.targetOneAllele(), "CA");

    assertEquals(result.numChromosomes(), 1006);
    assertEquals(result.numQueryOneAlleleChromosomes(), 516);
    assertEquals(result.numTargetOneAlleleChromosomes(), 475);
    assertEquals(result.numQueryAndTargetOneAlleleChromosomes(), 456);
    assertEquals(result.allelicCorrelation(), 0.434039, 1e-8);
    assertEquals(result.dprime(), 0.664331, 1e-8);
  }

  @Test
  public void toStringAndBack() {
    assertEquals(LdResult.fromLine(STRVAL).toString(), STRVAL);
    String other = "X,4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0";
    assertEquals(LdResult.fromLine(other).toString(), other);
  }

  @Test
  public void tooFewInputValues() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid LdResult line has 4 columns");
    LdResult.fromLine("too,few,input,columns");
  }

  @Test
  public void tooManyInputValues() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid LdResult line has 23 columns");
    LdResult.fromLine("too,many,cols,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23");
  }

  @Test
  public void noQueryChrom() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing queryChrom");
    LdResult.fromLine(",4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void invalidQueryStart() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("queryStart < 0: -4");
    LdResult.fromLine("X,-4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void invalidQuerySpan() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("queryEnd (3) <= queryStart (4)");
    LdResult.fromLine("X,4,3,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noQueryCloudId() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing queryCloudId");
    LdResult.fromLine("X,4,5,,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noQueryName() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing queryNames");
    LdResult.fromLine("X,4,5,1,,1,A,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void invalidQueryAltAlleles() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("queryNumAltAlleles <= 0: 0");
    LdResult.fromLine("X,4,5,1,Q,0,A,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noQueryZeroAllele() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing queryZeroAllele");
    LdResult.fromLine("X,4,5,1,Q,1,,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noQueryOneAllele() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing queryOneAllele");
    LdResult.fromLine("X,4,5,1,Q,1,A,,X,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noTargetChrom() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing targetChrom");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,,8,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void invalidTargetStart() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("targetStart < 0: -1");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,-1,9,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void invalidTargetSpan() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("targetEnd (8) <= targetStart (8)");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,8,6,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noTargetCloudId() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing targetCloudId");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,,T,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noTargetName() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing targetNames");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,,1,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void invalidTargetAltAlleles() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("targetNumAltAlleles <= 0: 0");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,T,0,C,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noTargetZeroAllele() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing targetZeroAllele");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,T,1,,G,99,45,45,45,1.0,1.0");
  }

  @Test
  public void noTargetOneAllele() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing targetOneAllele");
    LdResult.fromLine("X,4,5,1,Q,1,A,C,X,8,9,6,T,1,C,,99,45,45,45,1.0,1.0");
  }

  @Test
  public void invalidNumChromosomes() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("numChromosomes <= 0: 0");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,0,45,45,45,1.0,1.0");
  }

  @Test
  public void invalidQueryOneAlleleChromosomes() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid numQueryOneAlleleChromosomes");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,100,45,45,1.0,1.0");
  }

  @Test
  public void invalidTargetOneAlleleChromosomes() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid numTargetOneAlleleChromosomes");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,105,45,1.0,1.0");
  }

  @Test
  public void invalidNumQueryAndTargetOneAlleleChromosomes() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid numQueryAndTargetOneAlleleChromosomes");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,45,46,1.0,1.0");
  }

  @Test
  public void invalidCorrelation() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid allelicCorrelation:");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,45,45,3.0,1.0");
  }

  @Test
  public void invalidDprime() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid dprime:");
    LdResult.fromLine("X,4,5,1,Q,1,A,G,X,8,9,6,T,1,C,G,99,45,45,45,1.0,-1.1");
  }
}
