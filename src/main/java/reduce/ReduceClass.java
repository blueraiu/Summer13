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
package reduce;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.ReduceIO;



public class ReduceClass extends Reducer<Text,LongWritable,Text,Mutation> {
  
  private final static Text count = new Text("count"), empty = new Text("");
  private final ReduceIO  io;
  
  public ReduceClass() {
    this.io = new ReduceIO()
    .setOutputFormatClass( AccumuloOutputFormat.class )
    // TODO find a way if one exists to set these based on the classes parameterized types
    .setJobOutputKeyClass( Text.class )
    .setJobOutputValueClass( Mutation.class )
    .setNumberOfReduceTasks( 19 );
  }
  public ReduceIO getIO(){ return io; }
  
  /** Sum the term counts across partitions **/
  @Override public void reduce(Text key, Iterable<LongWritable> values, Context output){
    long sum = 0;
    while ( values.iterator().hasNext() ) { sum += values.iterator().next().get(); }
    Mutation m = new Mutation(key);
    m.put( count, empty, new Value( new Long(sum).toString().getBytes() ));
    try { output.write( null, m ); } // Table null is interpreted as the default table 
    catch (InterruptedException e) { e.printStackTrace(); } 
    catch (IOException e) { e.printStackTrace(); }
  }
}