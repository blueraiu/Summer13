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

package mapreduce;

import java.io.IOException;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.Parse;

import com.beust.jcommander.Parameter;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Word count for Accumulo output is an html table

public class Count2Html extends Configured implements Tool{
  
  public static void main( String[] args ) throws Exception {
    // Run the map reduce job
    int res = ToolRunner.run( CachedConfiguration.getInstance(), new Count2Html(), args );
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    // Create a custom commandline options parser
    Opts opts = new Opts();
    // Parse the options
    opts.parseArgs(Count2Html.class.getName(), args);
    
    // Create a new map reduce job
    Job job = new Job(getConf(), Count2Html.class.getName());
    job.setJarByClass(this.getClass());
    
    
    // Specify I/O format
    job.setInputFormatClass(AccumuloInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    // Specify the output directory 
    TextOutputFormat.setOutputPath(job, new Path(opts.outputDirectory));
    
    // Specify the mapper class
    job.setMapperClass(MapClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    // Secify the reducer class
    job.setReducerClass(ReduceClass.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(20);
        
    // Set the Accumulo connector, Zookeepers, i/o tables, and scan authorizations 
    opts.setAccumuloConfigs(job);
    //AccumuloOutputFormat.setDefaultTableName(job, opts.outputTable);
    
    job.waitForCompletion(true);
    return 0;
  }
  
  public static class MapClass extends Mapper<Key,Value,Text,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    @Override public void map(Key key, Value value, Context output) {
      // Just grab the text tokens for now
      if ( Parse.isTextToken( key ) ) {
        // Parse words in the rowID
        for ( String word : Parse.parseWordsFromKey( key ) ) {
            try { output.write(new Text(word), one); } 
            catch (InterruptedException e) { e.printStackTrace(); }
            catch (IOException e) { e.printStackTrace(); }   
        }
      }
    }
  }
  
  public static class ReduceClass extends Reducer<Text,IntWritable,Text,Text> {
    @Override public void reduce(Text key, Iterable<IntWritable> values, Context output){
      
      int sum = 0;
      while (values.iterator().hasNext()) { sum += values.iterator().next().get(); }
      
      // Html row with key and value each in a column
      try {
        output.write( new Text( "<tr><td>"+key.toString()+"</td>" ), new Text( "<td>"+sum+"</td></tr>" ) );
      } 
      catch (InterruptedException e) { e.printStackTrace(); } 
      catch (IOException e) { e.printStackTrace(); }     
    }
}
  
// Use the built-in client framework
  static class Opts extends ClientOnRequiredTable {
    // Add a parsable Jcommander option to indicate the output directory
    @Parameter(names="--output", description="output directory")
    String outputDirectory;
  }
}*/
