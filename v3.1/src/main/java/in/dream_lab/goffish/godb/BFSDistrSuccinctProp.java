
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
import org.apache.hama.commons.math.Tuple;
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
import in.dream_lab.goffish.godb.BFSDistrSuccinct.VertexMessageSteps;
import in.dream_lab.goffish.hama.succinctstructure.SuccinctArraySubgraph;
import in.dream_lab.goffish.hama.succinctstructure.SuccinctArrayVertex;





public class BFSDistrSuccinctProp extends
AbstractSubgraphComputation<BFSDistrPropSuccinctSubgraphState, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup {
	
	 public static final Log LOG = LogFactory.getLog(BFSDistrSuccinctProp.class);
	public BFSDistrSuccinctProp(String initMsg) {
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
//		static ScoreDoc[] hits;
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

		public  List<Long> hitList;
		public ArrayList<String> delimArray= new ArrayList<>();
		public ArrayList<String> propArray= new ArrayList<>();
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
			
			//add properties and delimiter list
			//for RGraph
			propArray.add("language");
			propArray.add("industry");
			propArray.add("contr");
			propArray.add("ispublic");
			propArray.add("follow");//added for Rgraph...remove for gplus
			
			delimArray.add("@");
			delimArray.add("$");
			delimArray.add("*");
			delimArray.add("^");
			delimArray.add("%");
			delimArray.add("|");
			
			
			 SuccinctArraySubgraph sg=(SuccinctArraySubgraph)getSubgraph();
			 
			 sg.setDelimArray(delimArray);
			 sg.setPropArray(propArray);
			
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
    
	  SuccinctArraySubgraph sg=(SuccinctArraySubgraph)getSubgraph();
  	//System.out.println("**********SUPERSTEPS***********:" + getSuperstep() +"Message List Size:" + messages.size());
		
		
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
//							 
								{
									
									queryMade=true;
//									LOG.info("Querying start");

									//TODO:uncomment this out for non Vid queries..IMPORTANT
									 hitList = sg.getVertexByProp(currentProperty, (String)currentValue, '@');
//									hitList=new ArrayList<>();
//									hitList.add(Long.parseLong(currentValue.toString()));
//									LOG.info("Querying end");
								
							}
							}
						}
						getSubgraph().getSubgraphValue().startPos=0;
					LOG.info("Starting Position:" + getSubgraph().getSubgraphValue().startPos);
					
					LOG.info("*******Querying done********:"+hitList.size());
						
						if(hitList.size()>0){
							for (int i=0;i<hitList.size();i++){

								long vid= hitList.get(i);
//							
									Long _vertexId = vid;
									String _message = "V:";
									
									getSubgraph().getSubgraphValue().forwardLocalVertexList.add( new VertexMessageSteps(_vertexId,_message, getSubgraph().getSubgraphValue().startPos, _vertexId, getSubgraph().getSubgraphId().get(), 0) );//TODO: remove storing of partition id
										
							}
						}
					

					
					
					}catch(Exception e){e.printStackTrace();}
					
		
					// TODO : else iteratively check for satisfying vertices
//					if ( queryPossible == true )
//					for(IVertex<MapValue,MapValue,LongWritable,LongWritable> vertex: getSubgraph().getLocalVertices()) {
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
					Iterator msgIterator=messages.iterator();
					while(msgIterator.hasNext()){
					  msgIterator.remove();
					  
					}
				}
//				System.out.println("Forward List size:" + getSubgraph().getSubgraphValue().forwardLocalVertexList.size());
				
			}
			
			
			// CHECK MSSG-PROCESS FORWARD-PROCESS BACKWARD
			if(getSuperstep()>=1) {
			
			// CHECK INCOMING MESSAGE, ADD VERTEX TO APPRT LIST
			// this is for the partially executed paths, which have been 
			// forwarded from a different machine
			  Iterator msgiterator=messages.iterator();
			if(msgiterator.hasNext()){
				for (IMessage<LongWritable, Text> message: messages){
					String m=message.getMessage().toString();
					if(m.contains("output();") ) {
						if ( Long.parseLong(m.split(Pattern.quote(";"))[3])  == getSubgraph().getSubgraphId().get() )
//							System.out.println("RECEIVED OUTPUT MESSAGE:" + new String(message.getData()));
							join(message);
						continue;
					}	
					getSubgraph().getSubgraphValue().forwardLocalVertexList.add( processMessage(message) ); 
					
				}

			}
			
			LOG.info(getSuperstep()+":"+getSubgraph().getSubgraphValue().forwardLocalVertexList.size());
			// PROCESS FORWARD LIST
			//System.out.println("FORWARD LIST:"+forwardLocalVertexList.isEmpty() +" REV LIST:"+revLocalVertexList.isEmpty() + "SGID:" + subgraph.getId() + " PID:" + partition.getId());
			while(!getSubgraph().getSubgraphValue().forwardLocalVertexList.isEmpty()) {
				
				VertexMessageSteps vertexMessageStep = getSubgraph().getSubgraphValue().forwardLocalVertexList.poll();

				if ( vertexMessageStep.stepsTraversed == getSubgraph().getSubgraphValue().Depth ){
					SuccinctArrayVertex<MapValue,MapValue,LongWritable,LongWritable> currentVertex = new SuccinctArrayVertex(new LongWritable(vertexMessageStep.vertexId),sg.getVertexBufferList(),sg.getEdgeBufferList(),'|');
					String[] str=currentVertex.getAllPropforVertex();
					StringBuilder _modifiedMsg = new StringBuilder("");
                    _modifiedMsg .append(vertexMessageStep.message).append(str[1]+","+str[2]+","+str[3]+"," + str[4]);
                    LOG.info("MODIFIEDMSG:" + _modifiedMsg.toString());
					if (vertexMessageStep.startSubgraphId == getSubgraph().getSubgraphId().get()) {
						if ( !getSubgraph().getSubgraphValue().resultsMap.containsKey(vertexMessageStep.startVertexId) )
							getSubgraph().getSubgraphValue().resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
						getSubgraph().getSubgraphValue().resultsMap.get(vertexMessageStep.startVertexId).forwardResultSet.add(vertexMessageStep.message);
						
					}	
					else {
						forwardOutputToSubgraph(1,vertexMessageStep);
						//output(partition.getId(), subgraph.getId(), "output();for();"+vertexMessageStep.message);
					}
					
					continue;
				}
				
				//Step nextStep = path.get(vertexMessageStep.stepsTraversed+1);
				
				SuccinctArrayVertex<MapValue,MapValue,LongWritable,LongWritable> currentVertex = new SuccinctArrayVertex(new LongWritable(vertexMessageStep.vertexId),sg.getVertexBufferList(),sg.getEdgeBufferList(),'|');
				
				StringBuilder _modifiedMsg = new StringBuilder("");
				String[] str=currentVertex.getAllPropforVertex();
//				if(str==null){
//					LOG.info("NULL Properties for vertex:"+ vertexMessageStep.vertexId);
//				}
				
							//local edges
							long count=0;
							Tuple<List<Long>,List<Long>> edges= currentVertex.getEdges(); 
							if((edges.getFirst().size() + edges.getSecond().size()) > 30) {
								//skipping this vertex if out degree greater than 17
								continue;
							}
							for( long edge: edges.getFirst() ) {
								count ++;
								long otherVertex = edge;
								StringBuilder _modifiedMessage = new StringBuilder("");
								_modifiedMessage.append(vertexMessageStep.message).append(str[1]+","+str[2]+","+str[3]+"," + str[4]).append("-->V:");
								LOG.info("MODIFIEDMSG:" + _modifiedMsg.toString());
									
								getSubgraph().getSubgraphValue().forwardLocalVertexList.add(new VertexMessageSteps(otherVertex,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId, vertexMessageStep.startSubgraphId, vertexMessageStep.startPartitionId));
								
									
							}
							
							
							//remote edges: changed for 1P to local queue
							for( long edge: edges.getSecond() ) {
								count ++;
								long otherVertex = edge;
								StringBuilder _modifiedMessage = new StringBuilder("");
								_modifiedMessage.append(vertexMessageStep.message).append(str[1]+","+str[2]+","+str[3]+"," + str[4]).append("-->V:");
								LOG.info("MODIFIEDMSG:" + _modifiedMsg.toString());
								getSubgraph().getSubgraphValue().forwardRemoteVertexList.add(new VertexMessageSteps(otherVertex,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId, vertexMessageStep.startSubgraphId, vertexMessageStep.startPartitionId));
//								getSubgraph().getSubgraphValue().forwardLocalVertexList.add(new VertexMessageSteps(otherVertex,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId, vertexMessageStep.startSubgraphId, vertexMessageStep.startPartitionId));
							}
							
							
							if(count==0){
//												LOG.info("Properties:"+ str);
				                                _modifiedMsg.append(vertexMessageStep.message).append(str[1]+","+str[2]+","+str[3]+"," + str[4]);
				                                LOG.info("MODIFIEDMSG:" + _modifiedMsg.toString());
				                                vertexMessageStep = new VertexMessageSteps(vertexMessageStep.vertexId, _modifiedMsg.toString(), vertexMessageStep.stepsTraversed, vertexMessageStep.startVertexId, vertexMessageStep.startSubgraphId, vertexMessageStep.startPartitionId);
				                                
//								if(vertexMessageStep.startSubgraphId!=getSubgraph().getSubgraphId().get()){
//									forwardOutputToSubgraph(1,vertexMessageStep);
//								}
//								else
//								{
//									if ( !getSubgraph().getSubgraphValue().resultsMap.containsKey(vertexMessageStep.startVertexId))
//										getSubgraph().getSubgraphValue().resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
//									getSubgraph().getSubgraphValue().resultsMap.get(vertexMessageStep.startVertexId).forwardResultSet.add(vertexMessageStep.message);
//								}
							}
//							System.out.println("*************getOutEdges***********:" + count);
								
			}
			
			

			LOG.info("Remote Messages:" + getSubgraph().getSubgraphValue().forwardRemoteVertexList.size());
			for(VertexMessageSteps stuff: getSubgraph().getSubgraphValue().forwardRemoteVertexList){
				// send message to all the remote vertices
				//FIXME:Verify the typecasting by talking to hama team
//				IRemoteVertex<MapValue,MapValue,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex)getSubgraph().getVertexById(new LongWritable(stuff.vertexId));//remoteVertex=null if remote inedge
				StringBuilder remoteMessage = new StringBuilder("for();");
				//remoteMessage.append(String.valueOf(stuff.vertexId.longValue())).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed) ;
				remoteMessage.append(String.valueOf(stuff.startVertexId)).append(";").append(String.valueOf(stuff.startSubgraphId)).append(";").append(stuff.startPartitionId).append(";").append(stuff.vertexId).append(";").append(stuff.message).append(";").append(stuff.stepsTraversed) ;
				Text remoteM = new Text(remoteMessage.toString());
//				LOG.info("RemoteVertexToSubgraph Vertex:" + stuff.vertexId + " Subgraph" + (long) sg.remotevertexToSubgraph.get(stuff.vertexId));
				sendMessage(new LongWritable((long) sg.remotevertexToSubgraph.get(stuff.vertexId)),remoteM);
//				LOG.info("Sending Remote Message:"+ remoteMessage.toString());
			}
			getSubgraph().getSubgraphValue().forwardRemoteVertexList.clear();
			
			//System.out.println("REMOTE MESSAGES SENT");
			
		}
		}
		
		
		if(getSuperstep()>=1)
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
  
 

