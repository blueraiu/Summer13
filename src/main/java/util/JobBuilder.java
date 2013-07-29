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

import mapreduce.AccumuloJob;
import mapreduce.AccumuloJob.JobProperties;
import mapreduce.MapClass;
import mapreduce.ReduceClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

/** Wrapper class for the Job class
 * TODO change raw types to generic types */
public class JobBuilder{

    private Configuration config;
    private String jobName;

    // Custom 
    private MapIO mapIO;
    private ReduceIO reduceIO;
    private Job job;
    
    
    // Backwards compatability
    private Class<? extends MapClass> mapperClass;
    private Class<? extends ReduceClass> reducerClass;
    private Class<? extends InputFormat> inputFormat;
    private Class<? extends OutputFormat> outputFormat;
    private Class<?> mapOutputKeyClass;
    private Class<?> mapOutputValueClass;
    private Class<?> jobOutputKeyClass;
    private Class<?> jobOutputValueClass;
    private Class<?> jar;
    private int numberOfReduceTasks =-1;
    
    
    private JobBuilder() {}
    private JobBuilder( Class<?> jar, String jobName, Configuration config ) {
      this.jar=jar;
      this.config=config;
      this.jobName=jobName;
    }
    public static JobBuilder newJobBuilderFromJobPropterties( JobProperties p ) {
      return new JobBuilder( JobProperties.getJar(), JobProperties.getName(), JobProperties.getConfig() );
    }
    /** Builder tool that makes an AccumuloJob from a mapio and reduceio
     * @return a new AccumuloJob
     * @throws IOException */
    public Job buildFromIO() throws IOException {
      if ( !nullIOParametersCheckPassed() ) { throw new NullPointerException("Not all required parameters are set"); }
      job = new AccumuloJob( config, jobName );
      job.setInputFormatClass( mapIO.getJobInputFormatClass() );
      job.setOutputFormatClass( reduceIO.getJobOutputFormatClass() );
      job.setMapperClass( mapperClass );
      job.setReducerClass( reducerClass );
      job.setMapOutputKeyClass( mapIO.getMapOutputKeyClass() );
      job.setMapOutputValueClass( mapIO.getMapOutputValueClass() );
      job.setOutputKeyClass( reduceIO.getJobOutputKeyClass() );
      job.setOutputValueClass( reduceIO.getJobOutputValueClass() );
      job.setNumReduceTasks( reduceIO.getNumberOfReduceTasks() );
      job.setJarByClass( jar );
      return job;
    } 

    public Job build() throws IOException { 
      if ( !nullParametersCheckPassed() ) { throw new NullPointerException("Not all required parameters are set"); }
      job = new Job( config, jobName );
      job.setInputFormatClass( inputFormat );
      job.setOutputFormatClass( outputFormat );
      job.setMapperClass( mapperClass );
      job.setReducerClass( reducerClass );
      job.setMapOutputKeyClass( mapOutputKeyClass );
      job.setMapOutputValueClass( mapOutputValueClass );
      job.setOutputKeyClass( jobOutputKeyClass );
      job.setOutputValueClass( jobOutputValueClass );
      job.setNumReduceTasks( numberOfReduceTasks );
      job.setJarByClass( jar );
      return job;
    }
    private boolean nullIOParametersCheckPassed() {
      assert config!=null : "You forgot to specify a configuration";
      assert mapIO.getJobInputFormatClass()!=null : "You forgot to specify an inputFormat";
      assert reduceIO.getJobOutputFormatClass()!=null : "You forgot to specify an outputFormat";
      assert mapperClass!=null : "You forgot to specify a mapper";
      assert reducerClass!=null : "You forgot to specify a reducer";
      assert mapIO.getMapOutputKeyClass()!=null : "You forgot to specify a mapOutputKeyClass";
      assert mapIO.getMapOutputValueClass()!=null : "You forgot to specify a mapOutputValueClass";
      assert reduceIO.getJobOutputKeyClass()!=null : "You forgot to specify a jobOutputKeyClass";
      assert reduceIO.getJobOutputValueClass()!=null : "You forgot to specify a jobOutputValueClass";
      assert reduceIO.getNumberOfReduceTasks()<0 : "You forgot to specify the numberOfReduceTasks";
      assert jar!=null : "You forgot to specify the jar";
      
      return true;
    }
    private boolean nullParametersCheckPassed() {
      assert config!=null : "You forgot to specify a configuration";
      assert inputFormat!=null : "You forgot to specify an inputFormat";
      assert outputFormat!=null : "You forgot to specify an outputFormat";
      assert mapperClass!=null : "You forgot to specify a mapper";
      assert reducerClass!=null : "You forgot to specify a reducer";
      assert mapOutputKeyClass!=null : "You forgot to specify a mapOutputKeyClass";
      assert mapOutputValueClass!=null : "You forgot to specify a mapOutputValueClass";
      assert jobOutputKeyClass!=null : "You forgot to specify a jobOutputKeyClass";
      assert jobOutputValueClass!=null : "You forgot to specify a jobOutputValueClass";
      assert numberOfReduceTasks<0 : "You forgot to specify the numberOfReduceTasks";
      assert jar!=null : "You forgot to specify the jar";
      return true;
    }
    
    public JobBuilder setConfiguration( Configuration config ) {
      this.config=config; return this;
    }
    public JobBuilder setJobName( String jobName ) {
      this.jobName=jobName; return this;
    }   
    public JobBuilder setInputFormatClass( Class<? extends InputFormat> inputFormat ) {
      this.inputFormat = inputFormat; return this;
    }
    public JobBuilder setOutputFormatClass( Class<? extends OutputFormat> outputFormat ) {
      this.outputFormat = outputFormat; return this;
    }
    public JobBuilder setMapper( Class<? extends MapClass> mapper ) {
      this.mapperClass = mapper; return this;
    }
    public JobBuilder setReducer( Class<? extends ReduceClass> reducer ) {
      this.reducerClass = reducer; return this;
    }
    public JobBuilder setMapOutputKeyClass( Class<?> mapOutputKeyClass ) {
      this.mapOutputKeyClass = mapOutputKeyClass; return this;
    }
    public JobBuilder setMapOutputValueClass( Class<?> mapOutputValueClass ) {
      this.mapOutputValueClass = mapOutputValueClass; return this;
    }
    public JobBuilder setJobOutputKeyClass( Class<?> jobOutputKeyClass ) {
      this.jobOutputKeyClass = jobOutputKeyClass; return this;
    }
    public JobBuilder setJobOutputValueClass( Class<?> jobOutputValueClass ) {
      this.jobOutputValueClass = jobOutputValueClass; return this;
    }
    public JobBuilder setNumberOfReduceTasks( int numberOfReduceTasks ) {
      this.numberOfReduceTasks=numberOfReduceTasks; return this;
    }
    public JobBuilder setJar( Class<?> jar ) {
      this.jar=jar; return this;
    }
    public JobBuilder setUpMapper( MapClass mapper ) {
      setMapper( mapper.getClass() );
      this.mapIO = mapper.getIO();
      return this;
    }
    public JobBuilder setUpReducer( ReduceClass reducer ) {
      setReducer( reducer.getClass() );;
      this.reduceIO = reducer.getIO();
      return this;
    }
}