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
package util;

import java.io.IOException;

import map.MapClass;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import reduce.ReduceClass;
import util.AccumuloJob.AccumuloOpts;
import util.AccumuloJob.JobProperties;


/** JubRunner for running Mapreduce jobs with Accumulo */
public class JobRunner extends Configured implements Tool{
  
  /** Fork, set static properties, and run the Tool
   * @param args - command line arguments
   * @throws Exception TODO when is this thrown */
  public static void main( String[] args ) throws Exception  { 
    //I <3 Java
    System.exit( ToolRunner.run( JobProperties.getConfig(), JobProperties.setProperties( new JobRunner() ), args ) ); 
  }
  /** Run the job 
   * @return 0 on successful 1 on error */
  public int run( String[] args ) throws Exception { 
    return parseOpts( args ).makeNewJob().setCustomAccumuloConfigs().waitForCompletion( true )? 0 : 1;
  }
  /** Construct a new, custom job with the JobBuilder using reflection =)
   * @return the Job
   * @throws IOException - TODO when is this thrown 
   * @throws ClassNotFoundException - if the provided class is not found
   * @throws IllegalAccessException 
   * @throws InstantiationException */
  public AccumuloJob makeNewJob() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException  {
    // TODO add options to change these on the fly
    return  (AccumuloJob) JobBuilder.newJobBuilderFromJobPropterties( new JobProperties() )
    .setUpMapper( (MapClass) Class.forName( AccumuloOpts.getOpts().mapClass ).newInstance() )
    .setUpReducer( (ReduceClass) Class.forName( AccumuloOpts.getOpts().reduceClass ).newInstance() )
    .buildFromIO();
  }
  /** Parse command line options
   * @param args command line arguments to parse
   * @return - custom JobRunner for convenience */
  public JobRunner parseOpts( String[] args ) {
    AccumuloOpts.getOpts().parseArgs( JobProperties.getName(), args );
    return this;
  }
}