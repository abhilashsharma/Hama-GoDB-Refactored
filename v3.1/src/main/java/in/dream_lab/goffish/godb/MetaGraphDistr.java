
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
import java.util.Map.Entry;
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





public class MetaGraphDistr extends
AbstractSubgraphComputation<MetaGraphDistrSubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup {
	
	 public static final Log LOG = LogFactory.getLog(MetaGraphDistr.class);
	public MetaGraphDistr(String initMsg) {
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

		enum Type{EDGE,VERTEX}
		enum Direction{OUT,IN}

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
		 * Initialize the class variables
		 * 
		 */
			//BFS query(with depth) example "label:string[toyota___rav_4]:4//0//163"
			
		private void init(Iterable<IMessage<LongWritable,Text>> messageList){

			//parsing query
			LOG.info("Pseudo partition id:" + (getSubgraph().getSubgraphId().get() >> 32));
			String arguments = Arguments;
			getSubgraph().getSubgraphValue().Arguments=arguments;
			LOG.info("***************ARGUMENTS************** :" +arguments);
			getSubgraph().getSubgraphValue().searchInstanceStart = new Integer(arguments.split(Pattern.quote("//"))[1]);
			getSubgraph().getSubgraphValue().searchInstanceEnd = new Integer(arguments.split(Pattern.quote("//"))[2]);
			String[] t=arguments.split(Pattern.quote("//"))[0].split(Pattern.quote(":"));
			getSubgraph().getSubgraphValue().path = new ArrayList<Step>();
			Object v=null;
			String typeAndValue = t[1];
			String type = typeAndValue.substring(0, typeAndValue.indexOf("["));
			if(type.equals("float")) {
				v = Float.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );
			}
			else if(type.equals("double")) { 
				v = Double.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

			}
			else if(type.equals("int")) { 
				v = Integer.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

			}
			else { 
				v = String.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

			}
			
			//populating data structures required for BFS
			getSubgraph().getSubgraphValue().Depth =Integer.parseInt(t[2]);
			getSubgraph().getSubgraphValue().path.add(new Step(Type.VERTEX,Direction.OUT,t[0],v));
			
			//initializing Lists
			getSubgraph().getSubgraphValue().forwardLocalVertexList = new LinkedList<VertexMessageSteps>();
			
			getSubgraph().getSubgraphValue().remoteSubgraphMap = new HashMap<Long, Long>();
			// read heuristics from memory
			
		}

		
	    void mountIndexToMemory()
	         {
	    	String Index_DIR="abhilash/index/";
	             try
	             {
	         		// Creating FileSystem object, to be able to work with HDFS	
	     			Configuration config = new Configuration();
	     			config.set("fs.default.name","hdfs://orion-00:19000/");
	     			FileSystem dfs = FileSystem.get(config);
	     			File LocalDir=new File("/tmp/index");
	     			if(!(LocalDir.exists())){
	     				System.out.println("Loading to /tmp done ");
	     			}
	     			else{
	     				System.out.println("Removing Existing index directory");
	     				FileUtils.deleteDirectory(LocalDir);
	     				System.out.println("Directory exists:" + LocalDir.exists());
	     			}
	     			
	     			dfs.copyToLocalFile(new Path(Index_DIR), new Path("/tmp"));
	             }
	             catch(Exception e){
	                 e.printStackTrace();
	             }

	             
	             
	         }

		
		
		

		//initialize lucene
		private void initLucene() throws InterruptedException, IOException{
			
			{
			  long pseudoPid=getSubgraph().getSubgraphId().get() >> 32;
				initDone = true;
				vertexIndexDir = new File(ConfigFile.basePath+ "/index/Partition"+pseudoPid+"/vertexIndex");
				vertexDirectory = FSDirectory.open(vertexIndexDir);
				analyzer = new StandardAnalyzer(Version.LATEST);
				indexReader  = DirectoryReader.open(vertexDirectory);
				indexSearcher = new IndexSearcher(indexReader);
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
		
		
		private void join(IMessage<LongWritable,Text> _message) {
		        long time=System.currentTimeMillis();
			String message =  _message.getMessage().toString();
			String[] split = message.split(Pattern.quote(";"));
			Long _startVertexId = Long.parseLong( split[2] );
			if ( !(getSubgraph().getSubgraphValue().resultsMap.containsKey(_startVertexId)) )
				getSubgraph().getSubgraphValue().resultsMap.put(_startVertexId, new ResultSet());
			if ( split[1].equals("for()") ) 
				getSubgraph().getSubgraphValue().resultsMap.get(_startVertexId).forwardResultSet.add(split[4]);
			else
				getSubgraph().getSubgraphValue().resultsMap.get(_startVertexId).revResultSet.add(split[4]);
			
			getSubgraph().getSubgraphValue().resultCollectionTime+=System.currentTimeMillis()-time;
		}
		
		
		
		private void forwardOutputToSubgraph(int direction,VertexMessageSteps step) {
		        long time=System.currentTimeMillis();
			String dir="for()";
			if(direction==0)
				dir="rev()";
			StringBuilder remoteMessage = new StringBuilder("output();"+dir+";");
			remoteMessage.append(step.startVertexId+";"+step.startSubgraphId+";"+step.message) ;
			Text remoteM = new Text(remoteMessage.toString());
			//remoteM.setTargetSubgraph(step.startPartitionId);
			sendMessage(new LongWritable(step.startSubgraphId), remoteM);
			//sendMessage(remoteM);

			getSubgraph().getSubgraphValue().resultCollectionTime+=System.currentTimeMillis()-time;
		}
		
		private VertexMessageSteps processMessage(IMessage<LongWritable,Text> _message){
			String message =  _message.getMessage().toString();
			String[] split = message.split(Pattern.quote(";"));
			Long _startVertexId = Long.parseLong( split[1] );
			Long _startSubgraphId = Long.parseLong( split[2] );
			Integer _startPartitionId = Integer.parseInt( split[3] );
			Long _vertexId = Long.parseLong(split[4] );
			Integer _steps = Integer.parseInt( split[6] );
			return new VertexMessageSteps(_vertexId, split[5] , _steps, _startVertexId , _startSubgraphId, _startPartitionId);
		}
		
		
  @Override
  public void compute(Iterable<IMessage<LongWritable,Text>> messages) {
    
	  HashMap<Long,Long> remoteSGEdgeCountMap = new HashMap<Long,Long>();
	  Long totalRemoteEdgeCount=0l;
	  for(IVertex<MapValue, MapValue, LongWritable, LongWritable> v: getSubgraph().getRemoteVertices()) {
		  IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex)v;
		  
		  Long count=remoteSGEdgeCountMap.get(remoteVertex.getSubgraphId().get());
		  if(count==null) {
			  count=0l;
		  }
		  remoteSGEdgeCountMap.put(remoteVertex.getSubgraphId().get(), count+1);
		  totalRemoteEdgeCount++;
	  }
	  
	  String remoteMapStr="";
	  
	  for (Entry<Long, Long> entry : remoteSGEdgeCountMap.entrySet())
	  {
	      remoteMapStr+=entry.getKey() + "," + entry.getValue();
	  }
	  LOG.info("SGID:" + getSubgraph().getSubgraphId().get() + ":" + getSubgraph().getLocalVertexCount() + ":" + totalRemoteEdgeCount + ":" + remoteMapStr);
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

	    if(!queryEnd){
	        queryEnd=true;
	        LOG.info("Ending Query Execution");
	    }
	  
		
		
		for(Map.Entry<Long, ResultSet> entry: getSubgraph().getSubgraphValue().resultsMap.entrySet()) {
			if (!entry.getValue().revResultSet.isEmpty())
				for(String partialRevPath: entry.getValue().revResultSet) {
					if (!entry.getValue().forwardResultSet.isEmpty())
						for(String partialForwardPath: entry.getValue().forwardResultSet) {
							LOG.info("ResultSet:" +partialRevPath+partialForwardPath);
							//output(partition.getId(), subgraph.getId(), partialRevPath+partialForwardPath); 
						}
					else{
						LOG.info("ResultSet:" +partialRevPath);
						//output(partition.getId(), subgraph.getId(), partialRevPath);
					}
				}
			else
				for(String partialForwardPath: entry.getValue().forwardResultSet) {
					LOG.info("ResultSet:" +partialForwardPath);
					//output(partition.getId(), subgraph.getId(), partialForwardPath); 
				}
		}
		
		
//		LOG.info("SetSize:" + getSubgraph().getSubgraphValue().resultsMap.size());
		LOG.info("Cumulative Result Collection:" + getSubgraph().getSubgraphValue().resultCollectionTime);
		
		//clearing Subgraph Value for next query
		clear();
	}


	public void clear(){
	  //for cleaning up the subgraph value so that Results could be cleared while Inedges won't be cleared so that it could be reused.
	  getSubgraph().getSubgraphValue().Arguments=null;
	  getSubgraph().getSubgraphValue().Depth=0;
	  getSubgraph().getSubgraphValue().forwardLocalVertexList.clear();
	  getSubgraph().getSubgraphValue().forwardRemoteVertexList.clear();
	  getSubgraph().getSubgraphValue().path.clear();
	  getSubgraph().getSubgraphValue().noOfSteps=0;
	  getSubgraph().getSubgraphValue().visitedVertices.clear();
	  getSubgraph().getSubgraphValue().resultsMap.clear();
	  queryMade=false;
	  queryStart=false;
          
	}
	

  
  
  }
  
 

