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

import org.apache.hadoop.mapreduce.InputFormat;

/** Bean Class representing the IO types for a Mapper TODO use parameterized types*/
public class MapIO{
  public MapIO() {}
  private Class<? extends InputFormat> inputFormat;
  private Class<?> mapOutputKeyClass;
  private Class<?> mapOutputValueClass;
  public  Class<? extends InputFormat> getJobInputFormatClass(){ return inputFormat; }
  public  Class<?> getMapOutputKeyClass(){ return mapOutputKeyClass; }
  public  Class<?> getMapOutputValueClass(){ return mapOutputValueClass; }
  public MapIO setInputFormatClass( Class<? extends InputFormat> inputFormat) { 
    this.inputFormat=inputFormat; return this; 
  }
  public MapIO setMapOutputKeyClass( Class<?> mapOutputKeyClass ) {
    this.mapOutputKeyClass = mapOutputKeyClass; return this;
  }
  public MapIO setMapOutputValueClass( Class<?> mapOutputValueClass ) {
    this.mapOutputValueClass = mapOutputValueClass; return this;
  }
}