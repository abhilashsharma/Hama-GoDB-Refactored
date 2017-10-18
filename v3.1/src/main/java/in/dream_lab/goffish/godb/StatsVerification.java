
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





public class StatsVerification extends
AbstractSubgraphComputation<BFSDistrSubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup {
	
	 public static final Log LOG = LogFactory.getLog(StatsVerification.class);
	public StatsVerification(String initMsg) {
		// TODO Auto-generated constructor stub
		
//		LOG.info("Subgraph Value:" + getSubgraph());
		Arguments=initMsg;
	}

	 String Arguments=null;
	 HashMap<Long,HashMap<Long,EdgeAttr>> InEdges=new HashMap<Long,HashMap<Long,EdgeAttr>>();
	 HashMap<Long,StringBuilder> MessagePacking=new HashMap<Long,StringBuilder>();
  
	 @Override
	  public void compute(Iterable<IMessage<LongWritable, Text>> messages)
	      throws IOException {
//		 long outDegree=0;
//		 long vertexMatch=0;
//		 String prop="country";
//		 String value="BG";
//		 for(IVertex<MapValue, MapValue, LongWritable, LongWritable> v: getSubgraph().getLocalVertices()) {
//			 
//			 if(v.getValue().get(prop).equals(value)) {
//				 vertexMatch++;
//				 for(IEdge<MapValue, LongWritable, LongWritable> edge:v.getOutEdges()) {
//					 outDegree++;
//				 }
//			 }
//		 }
//		 
//		 
//		 LOG.info("SGID:" + getSubgraph().getSubgraphId() + "," + vertexMatch + "," + outDegree);
		 //Degree distribution of US
//		 long vertexMatch=0;
//		 String prop="country";
//		 String value="US";
//		 for(IVertex<MapValue, MapValue, LongWritable, LongWritable> v: getSubgraph().getLocalVertices()) {
//			 
//			 if(v.getValue().get(prop).equals(value)) {
//				 vertexMatch++;
//				 long count=0;
//				 for(IEdge<MapValue, LongWritable, LongWritable> edge:v.getOutEdges()) {
//					 count++;
//					 
//				 }
//				 LOG.info("DegreeDistr:" + count);
//			 }
//		 
//		 
//		 }
		 //Degree distribution of US in path query involving BG->US
		 String prop1="country";
		 String value1="BG";
		 String prop="country";
		 String value="US";
		 
		 List<IVertex<MapValue, MapValue, LongWritable, LongWritable>> queueUS=new ArrayList<>();

		 //BG step
		 for(IVertex<MapValue, MapValue, LongWritable, LongWritable> v: getSubgraph().getLocalVertices()) {
		 
		 if(v.getValue().get(prop1).equals(value1)) {

			 for(IEdge<MapValue, LongWritable, LongWritable> edge:v.getOutEdges()) {
				 IVertex<MapValue, MapValue, LongWritable, LongWritable> Vertex=getSubgraph().getVertexById(edge.getSinkVertexId());
				 if(!Vertex.isRemote()) {
					 if(Vertex.getValue().get(prop).equals(value)) {
						 queueUS.add(Vertex);
					 }
				 }
				 
			 }
//			 LOG.info("DegreeDistr:" + count);
		 }
	 
	 
	 }
		 
		//US step 
		 for(IVertex<MapValue, MapValue, LongWritable, LongWritable> v: queueUS) {
		 
		 if(v.getValue().get(prop).equals(value)) {
			 long count=0;
			 for(IEdge<MapValue, LongWritable, LongWritable> edge:v.getOutEdges()) {
				 count++;
				 
			 }
			 LOG.info("DegreeDistr:" + count);
		 }
	 }
		 
		 
		 voteToHalt();
	  }//compute ends
	 
	 
  
	private boolean compareValuesUtil(Object o,Object currentValue){
		if( o.getClass().getName() != currentValue.getClass().getName()){return false;}
		if (o instanceof Float){
			return ((Float)o).equals((Float)currentValue);
		}
		else if (o instanceof Double){
			return ((Double)o).equals((Double)currentValue);
		}
		else if (o instanceof Integer){
			return ((Integer)o).equals((Integer)currentValue);
		}
		else{
			return ((String)o).equals((String)currentValue);
		}
		
	}


	@Override
	public void wrapup() {
		//Writing results back

	 
	}


 


	
	

  
  
  }
  
 

