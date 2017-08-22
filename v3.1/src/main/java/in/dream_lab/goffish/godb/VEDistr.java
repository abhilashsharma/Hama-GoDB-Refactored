
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
import org.apache.hadoop.io.MapWritable;
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
import in.dream_lab.goffish.godb.Step.Direction;
import in.dream_lab.goffish.godb.Step.Type;




public class VEDistr extends
AbstractSubgraphComputation<VEDistrSubgraphState, MapWritable, MapWritable, Text, LongWritable, LongWritable, LongWritable>
implements ISubgraphWrapup{
	
	 public static final Log LOG = LogFactory.getLog(VEDistr.class);
	public VEDistr(String initMsg) {
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
		 * Initialize the class variables
		 * 
		 */
			//BFS query(with depth) example "label:string[toyota___rav_4]:4//0//163"
			
		private void init(Iterable<IMessage<LongWritable,Text>> messageList){

			//parsing query
		  String arguments = Arguments;
	                getSubgraph().getSubgraphValue().Arguments=Arguments;
	        
	                
	            getSubgraph().getSubgraphValue().path = new ArrayList<Step>();
	                Type previousStepType = Type.EDGE;
	                for(String _string : arguments.split(Pattern.quote("//"))[0].split(Pattern.quote("@")) ){
	                        if(_string.contains("?")){
	                                if(previousStepType == Type.EDGE)
	                                        getSubgraph().getSubgraphValue().path.add(new Step(Type.VERTEX,null, null, null));
	                                previousStepType = Type.EDGE;
	                                String[] _contents = _string.split(Pattern.quote("?")); 
	                                String p = null ;
	                                Object v = null ;
	                                Direction d = (_contents[0].equals("out") ) ? Direction.OUT : Direction.IN;
	                                if ( _contents.length > 1 )     {
	                                        p = _contents[1].split(Pattern.quote(":"))[0];
	                                        String typeAndValue = _contents[1].split(Pattern.quote(":"))[1];
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
	                                }
	                                getSubgraph().getSubgraphValue().path.add(new Step(Type.EDGE, d, p, v));
	                        }
	                        else{
	                                previousStepType = Type.VERTEX;
	                                String p = _string.split(Pattern.quote(":"))[0];
	                                String typeAndValue = _string.split(Pattern.quote(":"))[1];
	                                Object v = null;
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

	                                getSubgraph().getSubgraphValue().path.add(new Step(Type.VERTEX,null, p, v));
	                        }
	                        

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
    
	  
//  	System.out.println("**********SUPERSTEPS***********:" + getSuperstep() +"Message List Size:" + messages.size());
		
		
		// STATIC ONE TIME PROCESSES
		{
			// LOAD QUERY AND INITIALIZE LUCENE
			if(getSuperstep() == 0){
	
	
				if(Arguments==null){
					//output(partition.getId(),subgraph.getId(),"START_ERROR:NO ARGUMENTS PROVIDED\tEXPECTED ARGUMENTS FORMAT\tvertexFilter@edgeDirection?edgeFilter@edgeDirection?edgeFilter@...|vertexFilter|edgeDirection?edgeFilter|...//instanceNumber\n");
					voteToHalt();
				}
				else
				{	
					init(messages);
					
					try{
						synchronized (initLock) {
							if ( !initDone )
								initInMemoryLucene();
						}
					}catch(Exception e){e.printStackTrace();}
					
				}
			}		
		}
		
		// RUNTIME FUNCTIONALITITES 
		{
			// COMPUTE-LOAD-INIT
			if(getSuperstep()==1){
				
			 
				if(!queryStart){
				  queryStart=true;
				LOG.info("Starting Query Execution");
				  queryEnd=false;
				}
				
				// LOAD START VERTICES
				{
					String currentProperty = null;
					Object currentValue = null;
					currentProperty = getSubgraph().getSubgraphValue().path.get(0).property; 
					currentValue = getSubgraph().getSubgraphValue().path.get(0).value;
					
					//check if the property is indexed** uncomment this if using indexes
					
					try{
						synchronized(queryLock){
							if(!queryMade){
//							        LOG.info("Property:" + currentProperty + " Value:" + currentValue + " ValueClass:" + currentValue.getClass());
								makeQuery(currentProperty,currentValue);
							}
						}
						getSubgraph().getSubgraphValue().startPos=0;
					LOG.info("Starting Position:" + getSubgraph().getSubgraphValue().startPos);
					
					LOG.info("*******Querying done********:"+hits.length);
						
						if(hits.length>0){
							for (int i=0;i<hits.length;i++){
								Document doc = indexSearcher.doc(hits[i].doc);
								if ( Long.valueOf(doc.get("subgraphid")) == getSubgraph().getSubgraphId().get() ){
									Long _vertexId = Long.valueOf(doc.get("id"));
									String _message = "V:"+String.valueOf(_vertexId);
//									System.out.println("Test Index:" + _message);
									getSubgraph().getSubgraphValue().resultsMap.add(_message);
										
								}
							}
						}
					

					
					
					}catch(Exception e){e.printStackTrace();}
					
		
					// TODO : else iteratively check for satisfying vertices
//					if ( queryPossible == true )
//					for(IVertex<MapWritable,MapWritable,LongWritable,LongWritable> vertex: getSubgraph().getLocalVertices()) {
//						if ( vertex.isRemote() ) continue;
//						//FIXME: change this for all type of attributes
//						
//						if ( compareValuesUtil(vertex.getValue().get(new Text(currentProperty)).toString(), currentValue.toString()) ) {
//							System.out.println("Vertex id:" + vertex.getVertexId().get() + "Property"+currentProperty +" Value:" + vertex.getValue().get(new Text(currentProperty)).toString());
//							String _message = "V:"+String.valueOf(vertex.getVertexId().get());
//							getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(vertex.getVertexId().get(),_message, getSubgraph().getSubgraphValue().startPos, vertex.getVertexId().get(), getSubgraph().getSubgraphId().get(),0) );
//							
//						}
//					}
					
					Iterator msgIter=messages.iterator();
                                        while(msgIter.hasNext()){
                                          msgIter.remove();
                                        }
				}
//				System.out.println("Forward List size:" + getSubgraph().getSubgraphValue().forwardLocalVertexList.size());
				voteToHalt();	
			}//END SUPERSTEP 1
			
			
			
		}//END RUNTIME
		
		
		
			

  	
  	
  }
  
  
	


	@Override
	public void wrapup() {
		//Writing results back
	  if(!queryEnd){
	        queryEnd=true;
	        LOG.info("Ending Query Execution");
	  }
		
		
	   for(String element:getSubgraph().getSubgraphValue().resultsMap){
	     
	     System.out.println("ResultSet:" + element);
	   }
		
		
//		LOG.info("SetSize:" + getSubgraph().getSubgraphValue().resultsMap.size());
		
		
		//clearing Subgraph Value for next query
		clear();
	}


	public void clear(){
	  //for cleaning up the subgraph value so that Results could be cleared while Inedges won't be cleared so that it could be reused.
	  getSubgraph().getSubgraphValue().Arguments=null;
	  getSubgraph().getSubgraphValue().Depth=0;

	  getSubgraph().getSubgraphValue().path.clear();
	  getSubgraph().getSubgraphValue().noOfSteps=0;
	  getSubgraph().getSubgraphValue().visitedVertices.clear();
	  getSubgraph().getSubgraphValue().resultsMap.clear();
	  queryMade=false;
	  queryStart=false;
          
	}
	

  
  
  }
  
 

