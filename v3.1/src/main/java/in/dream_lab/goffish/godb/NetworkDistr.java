
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
import org.apache.commons.lang.StringUtils;
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





public class NetworkDistr extends
AbstractSubgraphComputation<NetworkDistrSubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup {
	
	 public static final Log LOG = LogFactory.getLog(NetworkDistr.class);
	public NetworkDistr(String initMsg) {
		// TODO Auto-generated constructor stub
		
//		LOG.info("Subgraph Value:" + getSubgraph());
		Arguments=initMsg;
	}
		String Arguments=null;
		static File vertexIndexDir;
		static Directory vertexDirectory;
		static Analyzer analyzer;
		static IndexReader indexReader;
		static IndexSearcher indexSearcher;
		static BooleanQuery query;
		static ScoreDoc[] hits;
		static boolean initDone = false ;
		static boolean queryMade = false;
		private static final Object initLock = new Object();
		private static final Object queryLock = new Object();
		private static final Object startLock = new Object();
		private static final Object endLock = new Object();
		private static boolean queryStart=false;//later lock this when multithreaded
		private static boolean queryEnd=false;//later lock this when multithreaded

		private long predicateComputationTime=0;
		enum Type{EDGE,VERTEX}
		enum Direction{OUT,IN}
		 String message=StringUtils.leftPad("foobar", 100, '*');
		 Text txtMessage=new Text(message);
		 
		 ArrayList<String> forwardPaths=new ArrayList<String>();
		 ArrayList<String> reversePaths=new ArrayList<String>();
		 String dummyPath=StringUtils.leftPad("foobar", 60, '*');
		 long dummyVertex=432100;
		/**
		 * Class for storing the traversal path V->E->V->E->E.....
		 */
		public class Step{
			Type type = null;
			Direction direction = null;
			String property;
			Object value;
			Step(Type t,Direction d,String p,Object v){
				this.type = t;
				this.direction = d;
				this.property = p;
				this.value = v;
			}
		}

		/**
		 * Representative class to keep tab of next vertex to be processed 
		 */
		public class VertexMessageSteps{
			Long vertexId;
			String message;
			Integer stepsTraversed;
			Long startSubgraphId;
			Integer startPartitionId;
			Long startVertexId;
			VertexMessageSteps(Long _vertexId,String _message,Integer _stepsTraversed,Long _startVertexId,Long _startSubgraphId, Integer _startPartitionId){
				this.vertexId = _vertexId;
				this.message = _message;
				this.stepsTraversed = _stepsTraversed;
				this.startSubgraphId = _startSubgraphId;
				this.startVertexId = _startVertexId;
				this.startPartitionId = _startPartitionId;
			}
		} 
		
		class ResultSet{
			ArrayList<String> forwardResultSet;
			ArrayList<String> revResultSet;
			public ResultSet() {
				forwardResultSet = new ArrayList<String>();
				revResultSet = new ArrayList<String>();
			}
		}

		
		/**
                 * Initialize Lucene in memory
                 * searcher = new IndexSearcher (new RAMDirectory (indexDirectory)); 
                 */
                private void initInMemoryLucene() throws InterruptedException, IOException{
                     {
                       long pseudoPid=getSubgraph().getSubgraphId().get() >> 32;
                             initDone = true;
                             vertexIndexDir = new File(ConfigFile.basePath+ "/index/Partition"+pseudoPid+"/vertexIndex");
                             vertexDirectory = FSDirectory.open(vertexIndexDir);
                             analyzer = new StandardAnalyzer(Version.LATEST);
                             indexReader  = DirectoryReader.open(new RAMDirectory(vertexDirectory, IOContext.READ));//passing RAM directory to load indexes in memory
                             indexSearcher = new IndexSearcher(indexReader);
                     }
                  
                  
                }
                
  
                //updated Code to Integer attributes as well
                private void makeQuery(String prop,Object val) throws IOException{
                        {
                                queryMade = true;
                                if(val.getClass()==String.class){
                                query  = new BooleanQuery();
                                query.add(new TermQuery(new Term(prop, (String)val)), BooleanClause.Occur.MUST);
                                hits =  indexSearcher.search(query,40000).scoreDocs;
                                }
                                else if(val.getClass()==Integer.class)
                                {
                                        Query q = NumericRangeQuery.newIntRange(prop,(Integer)val, (Integer)val, true, true);
                                        hits =  indexSearcher.search(q,40000).scoreDocs;
                                }
                                
                        }
                }

		
	  

		
	
		
		
  @Override
  public void compute(Iterable<IMessage<LongWritable,Text>> messages) {
    
     
	  StringBuilder remoteMessage = new StringBuilder("");
	remoteMessage .append(String.valueOf(1231232l)).append(";").append(String.valueOf(1981231)).append(";").append(0).append(";").append(198131).append(";").append(2).append(";").append(4294967307l);
	
	if(getSubgraph().getSubgraphId().get()==19)
	{
		if(getSuperstep() < 10000) {
		
		for (int i=0 ;i<getSuperstep();i++) {
			sendMessage(new LongWritable(4294967307l), new Text(remoteMessage.toString()));
		}
		
		}
	}
	if(getSuperstep()==10000)
		voteToHalt();
  	
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
  
 

