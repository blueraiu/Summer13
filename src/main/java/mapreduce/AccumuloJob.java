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

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.beust.jcommander.Parameter;


/** Slightly enhanced Job for running Mapreduce Jobs that use Accumulo */
public class AccumuloJob extends Job{

  private static AccumuloOpts opts;
  public AccumuloJob(Configuration conf, String jobName) throws IOException { super(); }
  /**
   * Set the Connector, scan authorizations, table names, and custom options like iterators
   * @param opts - custom Options
   * @return - this job for convenience
   * @throws AccumuloSecurityException - when the user does not have the required auths to access Accumulo or a table
   * @throws TableNotFoundException - when a table is not found
   * @throws AccumuloException -TODO
   */
  public AccumuloJob setCustomAccumuloConfigs() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    opts.setAccumuloConfigs( this );
    AccumuloOutputFormat.setDefaultTableName( this, opts.outputTable ); 
    
    if ( opts.useCombiner ) {
      opts.getConnector().tableOperations().attachIterator( opts.tableName, new IteratorSetting( 15, "GlobalIndexUidCombiner", 
          "org.apache.accumulo.examples.wikisearch.iterator.GlobalIndexUidCombiner" ){});
    }
    //JobProperties.setName( opts.jobname );
    return this;
  }
  /** Added custom Job options
   * {@link ClientOnRequiredTable} */
  public static class AccumuloOpts extends ClientOnRequiredTable {
    private AccumuloOpts() {}
    @Parameter(names="--output", description="output table")
    String outputTable;
    //@Parameter(names="--jobname", description="Job name")
    //String jobname;
    @Parameter(names="--mapClass", description="Map class", required = true)
    String mapClass;
    @Parameter(names="--reduceClass", description="Reduce Class", required = true)
    String reduceClass;
    @Parameter(names="--useCombiner", description="Whether or not to use a combiner")
    boolean useCombiner=false;
    
    public static AccumuloOpts getOpts(){ 
      if ( opts!=null ){ return opts; }
      return opts = new AccumuloOpts();
    } 
  } 
  
  // Job Properties Bean
  public static class JobProperties{
    private static Class<?> jar;
    private static String name;
    private static Configuration config;
    public static Class<?> getJar(){ return jar; }
    public static String getName(){ return name; }
    public static Configuration getConfig(){ return config; }
    public static  void setJar( Class<?> jar ) { JobProperties.jar=jar; }
    public static void setName( String name ) { JobProperties.name=name; }
    public static void setConfig( Configuration config ) { JobProperties.config=config; }
    
    /** Convenience method for setting properties
     * @param jobClass - the job used to set the jar and name
     * @return the JobRunner, mainly for convenience
     */
    public static  JobRunner setProperties( JobRunner jobClass ) {
      JobProperties.setJar( jobClass.getClass() );
      JobProperties.setName( JobProperties.jar.getName() );
      JobProperties.setConfig( CachedConfiguration.getInstance() );
      return jobClass;
    }
  }
}
