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

import org.apache.accumulo.core.data.Key;

/**
 * 
 */
public class Parse {
  /**
   * Keeping it simple for now, naive parser that includes things like word.word but also word.jpg and word's 
   * TODO look more closely at the way tokens are created and make a better word parser
   * @param k Key to be parsed
   * @return a String array of 'words'
   */
  public static String[] parseWordsFromKey( Key k ) {
    String row = k.getRow().toString();
    row = row.replaceAll("[^a-zA-Z]", " ");// Replace any non letter characters with a space
    String[] words = row.trim().split("\\s+");
    return words;
  }
  public static boolean isTimestamp( Key k ) {
    return ( k.getColumnFamily().toString().equals("TIMESTAMP") )? true:false;
  }
  /**
   * Check whether the given row is classified as TEXT
   * @param k Key to check
   * @return true if the Key's column family is TEXT, false otherwise
   */
  public static boolean isTextToken( Key k ) {
    return ( k.getColumnFamily().toString().equals("TEXT")) ? true:false;
  }
}
