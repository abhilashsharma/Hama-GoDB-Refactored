
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





public class EdgeTest extends
AbstractSubgraphComputation<BFSDistrSubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup {
	
	 public static final Log LOG = LogFactory.getLog(EdgeTest.class);
	public EdgeTest(String initMsg) {
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
	  
	   
	   
	   if (getSuperstep()==0) {
             // GATHER HEURISTICS FROM OTHER SUBGRAPHS
             

             
       
            
     
             
             String m="";
             
             for(IVertex<MapValue, MapValue, LongWritable, LongWritable> sourceVertex:getSubgraph().getLocalVertices())
             for(IEdge<MapValue, LongWritable, LongWritable> edge : sourceVertex.getOutEdges()) {
                     
                     IVertex<MapValue, MapValue, LongWritable, LongWritable> sinkVertex=getSubgraph().getVertexById(edge.getSinkVertexId());
             //if sink vertex is not remote then add inedge to appropriate data structure, otherwise send source value to remote partition
             if(!sinkVertex.isRemote())
             {
                     if(InEdges.containsKey(sinkVertex.getVertexId().get()))
                     {
                             if(!InEdges.get(sinkVertex.getVertexId().get()).containsKey(sourceVertex.getVertexId().get()))
                             {
                                     
                                     
//                                   ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                                     EdgeAttr attr= new EdgeAttr("relation","null" /*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);
                                     InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
                                     //System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
                             }
                             
                     }
                     else
                     {
//                           ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                             EdgeAttr attr= new EdgeAttr("relation", "null"/*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);                                
                             InEdges.put(sinkVertex.getVertexId().get(), new HashMap<Long,EdgeAttr>());
                             InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
                             //System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
                             
                     }
                     
                     //System.out.println(edge.getSource().getId() + " -->" + edge.getSink().getId());
             }
             else
             { //send message to remote partition
             
             //TODO: generalize this for all attributes
                     IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapValue, MapValue, LongWritable, LongWritable, LongWritable>)sinkVertex;
                     remoteVertex.getSubgraphId().get();
             if(!MessagePacking.containsKey(remoteVertex.getSubgraphId().get()))
                     MessagePacking.put(remoteVertex.getSubgraphId().get(),new StringBuilder("#|" + sourceVertex.getVertexId().get()  + "|" + sinkVertex.getVertexId().get() + "|" + "relation" + ":"  +"null" /*subgraphProperties.getValue("relation").toString()*/+"|" + edge.getEdgeId().get()+"|" + getSubgraph().getSubgraphId().get() + "|" +0));
             else{
                     MessagePacking.get(remoteVertex.getSubgraphId().get()).append("$").append("#|").append(sourceVertex.getVertexId().get()).
                                                             append("|").append(sinkVertex.getVertexId().get()).
                                                             append("|").append("relation").append(":").append("null" /*subgraphProperties.getValue("relation").toString()*/).
                                                             append("|").append(edge.getEdgeId().get()).
                                                             append("|").append(getSubgraph().getSubgraphId().get()).
                                                             append("|").append(0);
                     
             }
             
             
             
             }
             
             
             
             
             
 }
     
     //Sending packed messages by iterating through MessagePacking Hashmap
     for(Map.Entry<Long,StringBuilder> remoteSubgraphMessage: MessagePacking.entrySet()){
             Text msg = new Text(remoteSubgraphMessage.getValue().toString());
     
     sendMessage(new LongWritable(remoteSubgraphMessage.getKey()),msg);
     }
     

             
     }

     //subgraphId/20:attr?21,12,23|attr?12,12
     else if ( getSuperstep()==1 ) {
     //TODO :remove this hack
     for (IMessage<LongWritable, Text> _message: messages){
             
             String message = _message.getMessage().toString();

             
             String[] SubgraphMessages=message.split(Pattern.quote("$"));
             for(String subgraphMessage:SubgraphMessages){
                     String[] values = subgraphMessage.split(Pattern.quote("|"));
              long Source=Long.parseLong(values[1]);
              long Sink=Long.parseLong(values[2]);
              String[] attr_data=values[3].split(":");
              if(InEdges.containsKey(Sink))
               {
                      EdgeAttr attr= new EdgeAttr(attr_data[0],attr_data[1],Long.parseLong(values[4]),true,Long.parseLong(values[5]));                               
                      InEdges.get(Sink).put(Source, attr);
               }
              else
               {
                      EdgeAttr attr= new EdgeAttr(attr_data[0],attr_data[1],Long.parseLong(values[4]),true,Long.parseLong(values[5]));   
                      InEdges.put(Sink, new HashMap<Long,EdgeAttr>());
                      InEdges.get(Sink).put(Source, attr);
               }
             }
                     
             
             
                     
     }
    
     
}

	   
	   
	   
	   
	   if(getSuperstep()==2){
	   int count=0;
	   for(IVertex<MapValue, MapValue, LongWritable, LongWritable> v:getSubgraph().getLocalVertices()){
	     for(IEdge<MapValue, LongWritable, LongWritable> e: v.getOutEdges()){
	       count++;
	     }
	   }
	  }
	  else if(getSuperstep()==3){
	    int count=0;
	    for(IVertex<MapValue, MapValue, LongWritable, LongWritable> v:getSubgraph().getLocalVertices()){
	      HashMap<Long,EdgeAttr> attr = InEdges.get(v.getVertexId().get());
	      if(attr!=null)
	      for(Map.Entry<Long, EdgeAttr> edgeMap: attr.entrySet()){
	        count++;
	      }
	      
	    }
	    
	  }
	  else{
	    voteToHalt();
	  }
	    
	  }
	 
	 
  
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
  
 

