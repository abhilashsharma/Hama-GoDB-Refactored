
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.dream_lab.goffish.godb;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.Collection;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;





public class SuccintDataFormat extends
AbstractSubgraphComputation<BFSDistrPropSubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup {
	
	 public static final Log LOG = LogFactory.getLog(SuccintDataFormat.class);
	public SuccintDataFormat(String initMsg) {
		// TODO Auto-generated constructor stub
		
//		LOG.info("Subgraph Value:" + getSubgraph());
		Arguments=initMsg;
	}
		String Arguments;


		
		
		
  @Override
  public void compute(Iterable<IMessage<LongWritable,Text>> messages) {
    
	  
  	//System.out.println("**********SUPERSTEPS***********:" + getSuperstep() +"Message List Size:" + messages.size());
		
		
		// STATIC ONE TIME PROCESSES
		{
			// LOAD QUERY AND INITIALIZE LUCENE
			int count=0;
			int ecount=0;
			if(getSuperstep() == 0){
				for(IVertex<MapValue, MapValue, LongWritable, LongWritable> v : getSubgraph().getLocalVertices()) {
//					count++;
//					if(count==1000) {
//						break;
//					}
					LOG.info("VERTEXDATA:" + getSubgraph().getSubgraphId().toString()+"#" + v.getValue().get("patid") + "@" + v.getValue().get("country") + "$" + v.getValue().get("nclass") + "|" );
					StringBuilder str=new StringBuilder();
					ecount=0;
					
					for(IEdge<MapValue, LongWritable, LongWritable> e:v.getOutEdges()) {
						
						String sink= ""+e.getSinkVertexId();
						str.append((ecount==0)?"":":");
						str.append(sink);
						System.out.println("STR:" + str.toString());
						ecount++;
					}
					System.out.println("ECOUNT:" + ecount);
					System.out.println("EDGEDATA:"+ getSubgraph().getSubgraphId().toString()+"#" + v.getVertexId() + "@" + ecount + "%" + str.toString() + "|" );
				}
			}
	
		}
		
				voteToHalt();

  	
  	
  }
  
  
	

	@Override
	public void wrapup() {
		//Writing results back

	  
		
		
//		for(Map.Entry<Long, ResultSet> entry: getSubgraph().getSubgraphValue().resultsMap.entrySet()) {
//			if (!entry.getValue().revResultSet.isEmpty())
//				for(String partialRevPath: entry.getValue().revResultSet) {
//					if (!entry.getValue().forwardResultSet.isEmpty())
//						for(String partialForwardPath: entry.getValue().forwardResultSet) {
//							LOG.info("ResultSet:" +partialRevPath+partialForwardPath);
//							//output(partition.getId(), subgraph.getId(), partialRevPath+partialForwardPath); 
//						}
//					else{
//						LOG.info("ResultSet:" +partialRevPath);
//						//output(partition.getId(), subgraph.getId(), partialRevPath);
//					}
//				}
//			else
//				for(String partialForwardPath: entry.getValue().forwardResultSet) {
//					LOG.info("ResultSet:" +partialForwardPath);
//					//output(partition.getId(), subgraph.getId(), partialForwardPath); 
//				}
//		}
		
		
//		LOG.info("SetSize:" + getSubgraph().getSubgraphValue().resultsMap.size());
		
		//clearing Subgraph Value for next query
	}


		

  
  
  }
  
 

