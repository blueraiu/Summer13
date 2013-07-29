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
package mapreduce;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.examples.wikisearch.iterator.GlobalIndexUidCombiner.UidListEncoder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MapIO;
import util.Parse;


public class MapClass extends Mapper<Key,Value,Text,LongWritable> {
  
  public LongWritable countWritable;
  public final static LongWritable oneWritable = new LongWritable(1);
  public MapIO  io;
 
  public MapClass() {
    this.io = new MapIO()
    .setInputFormatClass( AccumuloInputFormat.class )
    // TODO find a way if one exists to set these based on the classes parameterized types
    .setMapOutputKeyClass( Text.class )
    .setMapOutputValueClass( LongWritable.class );
  }
  public MapIO getIO(){ return io; }
  
  /** Get the count from the protocol buffer Uid.List and output it for each word in the token **/
  @Override public void map(Key key, Value value, Context output) {
    if ( Parse.isTextToken( key )) {  
      long count = new UidListEncoder().decode( value.get() ).getCOUNT();
      countWritable = ( count==1? oneWritable : new LongWritable( count ) );// Little optimization
      for ( String word : Parse.parseWordsFromKey( key ) ) {
          try { output.write( new Text( word ), countWritable ); } 
          catch (InterruptedException e) { e.printStackTrace(); }
          catch (IOException e) { e.printStackTrace(); }   
      }
    }
  }
}
