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
package batch;

import static java.lang.System.out;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.BatchScannerOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;

/**
 * Simple App to print an Accumulo table as an Html table
 */
public class BatchScan2Html {
  
  private static  ClientOnRequiredTable opts;
  private static  BatchScannerOpts bsOpts;
  private static  Connector connector;
  private static  BatchScanner batchScanner;
  private static long bytesWritten = 0, startTime, crudeRunTime;

  public BatchScan2Html(){}
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException, URISyntaxException {
    initialize( args );
    writeAccumuloTableToHdfsAsHtml();    
  }
  public static void initialize( String[] args ) throws TableNotFoundException, AccumuloException, AccumuloSecurityException  {
    opts = new ClientOnRequiredTable();
    bsOpts = new BatchScannerOpts();
    opts.parseArgs( BatchScan2Html.class.getName(), args, bsOpts );
    connector = ( opts.getConnector() ); 
    batchScanner = getConfiguredBatchScanner();
  }  
  public static BatchScanner getConfiguredBatchScanner() throws TableNotFoundException {
    Collection<Range> ranges = new ArrayList<Range>(
        Arrays.asList( new Range( (Key)null,(Key)null) ) );
    //Iterable<Entry<Key,Value>> 
    BatchScanner bscan = connector.createBatchScanner( opts.tableName, opts.auths, bsOpts.scanThreads );
    bscan.setTimeout( bsOpts.scanTimeout, TimeUnit.MILLISECONDS );
    //TODO add Jcommander optioins for ranges and columns
    bscan.setRanges( ranges );
    bscan.fetchColumnFamily( new Text( "TEXT" ) );
    bscan.addScanIterator( new IteratorSetting( 15, "GlobalIndexUidCombiner", 
        "org.apache.accumulo.examples.wikisearch.iterator.GlobalIndexUidCombiner" ){});
    //bscan.fetchColumn();
    return bscan;
  }  
  public static void writeAccumuloTableToHdfsAsHtml() throws IOException, URISyntaxException  {
    Configuration configuration = new Configuration();
    //TODO add options for URI and output Path
    FileSystem hdfs = FileSystem.get( new URI( "hdfs://n001:54310" ), configuration );
    Path file = new Path("hdfs://n001:54310/s2013/batch/table.html");
    //TODO add option to override file default: true
    if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
    startTime = System.currentTimeMillis();
    OutputStream os = hdfs.create( file,
        new Progressable() {
          public void progress() {
            // TODO add a better progress descriptor
            crudeRunTime = System.currentTimeMillis() - startTime;
            out.println("...bytes written: [ "+bytesWritten+" ]");
            out.println("...bytes / second: [ "+(bytesWritten/crudeRunTime)*1000+" ]");
          } });
    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
    //  TODO add option for table id { example }
    writeHtmlTableHeader( br, "example", new ArrayList<String>(
         Arrays.asList( "Row ID", "Column Family", "Column Qualifier", "Column Visibility", "Timestamp", "Value" )));
    writeHtmlTableBody( br );
    out.println("Total bytes written: "+bytesWritten);
    out.println("Total crude time: "+crudeRunTime/1000);
    br.close();
    hdfs.close();
  }
  public static void writeHtmlTableHeader( Writer w, String tableID, List<String> columnHeaders ) throws IOException {
    w.write("<table id=\""+tableID+"\"");
      w.write("<thead>");
        w.write("<tr>");
        for( String header : columnHeaders ) { w.write("<th>"+header+"</th>"); }
        w.write("</tr>");
      w.write("</thead>");

  }
  public static void writeHtmlTableBody( Writer w ) throws IOException {
      w.write("<tbody>");
        for( Entry<Key,Value> e : batchScanner ) {
          w.write("<tr>");
            w.write("<td>"+e.getKey().getRow()+"</td>");
            w.write("<td>"+e.getKey().getColumnFamily()+"</td>"); 
            w.write("<td>"+e.getKey().getColumnQualifier()+"</td>");
            w.write("<td>"+e.getKey().getColumnVisibility()+"</td>");
            w.write("<td>"+e.getKey().getTimestamp()+"</td>");
            w.write("<td>"+e.getValue()+"</td>");
            countBytesWritten( e );
          w.write("</tr>");
        }
      w.write("</tbody>");
    w.write("</table>");
  }
  public static void countBytesWritten( Entry<Key, Value> e ) {
    bytesWritten += e.getKey().getRow().getBytes().length +
        e.getKey().getColumnFamily().getBytes().length +
        e.getKey().getColumnQualifier().getBytes().length +
        e.getKey().getColumnVisibility().getBytes().length +
        8 +// 8 bytes in 64 bit long timestamp
        e.getValue().get().length;
  }
}