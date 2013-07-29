package util;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.Arrays;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Paramerterized test class for Parse.parseWordsFromKey method
 */
@RunWith(value = Parameterized.class)
public class ParseTest {
  private Key row;
  private String[] expected;
  
  public ParseTest(Key row, String[] expected) {
    this.row=row;
    this.expected=expected;
  }

  
  @Parameters   //.Parameters(name = "{index}: parse({0})={1}")
  public static Collection<Object[]> data(){
    Object[][] data = new Object[][] {
        { new Key("\\x00"), new String[] {"x"} },
        { new Key("apple&mdash"), new String[] {"apple","mdash"} },
        { new Key("apple.11"), new String[] {"apple"} }  
    };
    /* run a subset:
     * Collection<Object[]> c = Arrays.asList( data ).subList( startInclusive, stopExclusive );
     */
    return Arrays.asList( data );
  }
  
  @Test public void testParseWordsFromKey() {
    
    Assert.assertArrayEquals( this.expected, Parse.parseWordsFromKey( this.row ) );
    
    //System.out.print( new Key("\\x00").getRow().toString() +" <> "+Parse.parseWordsFromKey( new Key("\\x00") )+"\n");
    System.out.print( "Key: \""+this.row.getRow().toString()+"\" Output: \""+
        Arrays.toString( this.expected )+"\" <> \""+
        Arrays.toString( Parse.parseWordsFromKey( this.row ) )+"\"\n");
  }
  
  @Test public void testIsTimestamp() {
    Key k = new Key( new Text("Key") );
    Key timestampCF = new Key( k.getColumnFamily(), new Text("TIMESTAMP") );
    Assert.assertEquals( false, Parse.isTimestamp( k ) );
    Assert.assertEquals( true, Parse.isTimestamp( timestampCF ) );
  }
  
}
