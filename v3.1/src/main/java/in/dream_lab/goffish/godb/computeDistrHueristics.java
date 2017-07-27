package in.dream_lab.goffish.godb;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.EdgeAttr;

public class computeDistrHueristics 
extends AbstractSubgraphComputation<pathDistrSubgraphState, MapWritable, MapWritable, Text, LongWritable, LongWritable, LongWritable> 
implements ISubgraphWrapup {

	
  public computeDistrHueristics(String initMsg) {
    // TODO Auto-generated constructor stub
    
//  LOG.info("Subgraph Value:" + getSubgraph());
    Arguments=initMsg;
}
	
	
  String Arguments=null;
  Hueristics subgraphHueristics = new Hueristics(); 	
  public static final Log LOG = LogFactory.getLog(computeDistrHueristics.class);
  private static boolean gcCalled=false;
	FileOutputStream fos;
	ObjectOutputStream oos;
	String heuristicsBasePath = ConfigFile.basePath +"heuristics/hue_";
	
	
	Set<String> vertexProperties=new HashSet<String>();
	Set<String> edgeProperties=new HashSet<String>();
	
				
	
				long time;
				
	@Override
	public void compute(Iterable<IMessage<LongWritable, Text>> messageList) {
		
		// SUPERSTEP TO SEND MESSAGE ALONG REMOTE EDGES
		// THIS IS TO NOTIFY THE REMOTE VERTICES IN THE 
		// OTHER SUBGRAPH THAT THEY HAVE AN INCOMING EDGE
		// REMOTE SOURCE OF INCOMING EDGES ARE MAINTAINED 
		// IN A TEMPORARY MAP 
		//TODO: REMOVE THIS HACK AND ADD ACTUAL SUPPORT FOR THIS
		
		if ( getSuperstep()==0) {
			//send messages along remote edges
			vertexProperties.add("patid");
			vertexProperties.add("nclass");
			vertexProperties.add("country");
		      if(getSubgraph().getSubgraphValue().InEdges==null){
                        //Logic to Accumulate inedges
                          getSubgraph().getSubgraphValue().InEdges=new HashMap<Long,HashMap<Long,EdgeAttr>>();  
                        time=System.currentTimeMillis();
                
                        
                        String m="";
                        
                        for(IVertex<MapWritable, MapWritable, LongWritable, LongWritable> sourceVertex:getSubgraph().getLocalVertices())
                        for(IEdge<MapWritable, LongWritable, LongWritable> edge : sourceVertex.getOutEdges()) {
                                
                                IVertex<MapWritable, MapWritable, LongWritable, LongWritable> sinkVertex=getSubgraph().getVertexById(edge.getSinkVertexId());
                        //if sink vertex is not remote then add inedge to appropriate data structure, otherwise send source value to remote partition
                        if(!sinkVertex.isRemote())
                        {
                                if(getSubgraph().getSubgraphValue().InEdges.containsKey(sinkVertex.getVertexId().get()))
                                {
                                        if(!getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).containsKey(sourceVertex.getVertexId().get()))
                                        {
                                                
                                                
//                                              ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                                                EdgeAttr attr= new EdgeAttr("relation","null" /*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);
                                                getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
                                                //System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
                                        }
                                        
                                }
                                else
                                {
//                                      ISubgraphObjectProperties subgraphProperties = subgraphInstance.getPropertiesForEdge(edge.getId());
                                        EdgeAttr attr= new EdgeAttr("relation", "null"/*subgraphProperties.getValue("relation").toString()*/,edge.getEdgeId().get(),false,null);                                
                                        getSubgraph().getSubgraphValue().InEdges.put(sinkVertex.getVertexId().get(), new HashMap<Long,EdgeAttr>());
                                        getSubgraph().getSubgraphValue().InEdges.get(sinkVertex.getVertexId().get()).put(sourceVertex.getVertexId().get(), attr);
                                        //System.out.println("Accumulation inedge for edge "+ edge.getId() + " Value " + subgraphProperties.getValue("relation").toString() );
                                        
                                }
                                
                                //System.out.println(edge.getSource().getId() + " -->" + edge.getSink().getId());
                        }
                        else
                        { //send message to remote partition
                        
                        //TODO: generalize this for all attributes
                                IRemoteVertex<MapWritable,MapWritable,LongWritable,LongWritable,LongWritable> remoteVertex = (IRemoteVertex<MapWritable, MapWritable, LongWritable, LongWritable, LongWritable>)sinkVertex;
                                remoteVertex.getSubgraphId().get();
                        if(!getSubgraph().getSubgraphValue().MessagePacking.containsKey(remoteVertex.getSubgraphId().get()))
                                getSubgraph().getSubgraphValue().MessagePacking.put(remoteVertex.getSubgraphId().get(),new StringBuilder("#|" + sourceVertex.getVertexId().get()  + "|" + sinkVertex.getVertexId().get() + "|" + "relation" + ":"  +"null" /*subgraphProperties.getValue("relation").toString()*/+"|" + edge.getEdgeId().get()+"|" + getSubgraph().getSubgraphId().get() + "|" +0));
                        else{
                                getSubgraph().getSubgraphValue().MessagePacking.get(remoteVertex.getSubgraphId().get()).append("$").append("#|").append(sourceVertex.getVertexId().get()).
                                                                        append("|").append(sinkVertex.getVertexId().get()).
                                                                        append("|").append("relation").append(":").append("null" /*subgraphProperties.getValue("relation").toString()*/).
                                                                        append("|").append(edge.getEdgeId().get()).
                                                                        append("|").append(getSubgraph().getSubgraphId().get()).
                                                                        append("|").append(0);
                                
                        }
                        
                        
                        
                        }
                        
                        
                        
                        
                        
            }
                
                //Sending packed messages by iterating through MessagePacking Hashmap
                for(Map.Entry<Long,StringBuilder> remoteSubgraphMessage: getSubgraph().getSubgraphValue().MessagePacking.entrySet()){
                        Text msg = new Text(remoteSubgraphMessage.getValue().toString());
                
                sendMessage(new LongWritable(remoteSubgraphMessage.getKey()),msg);
                }
                
        }
			
        	
		}	
		
		
		if(  getSuperstep()==1) {
		  for (IMessage<LongWritable, Text> _message: messageList){
                    
                    String message = _message.getMessage().toString();

                    
                    String[] SubgraphMessages=message.split(Pattern.quote("$"));
                    for(String subgraphMessage:SubgraphMessages){
                            String[] values = subgraphMessage.split(Pattern.quote("|"));
                     long Source=Long.parseLong(values[1]);
                     long Sink=Long.parseLong(values[2]);
                     String[] attr_data=values[3].split(":");
                     if(getSubgraph().getSubgraphValue().InEdges.containsKey(Sink))
                      {
                             EdgeAttr attr= new EdgeAttr(attr_data[0],attr_data[1],Long.parseLong(values[4]),true,Long.parseLong(values[5]));                               
                             getSubgraph().getSubgraphValue().InEdges.get(Sink).put(Source, attr);
                      }
                     else
                      {
                             EdgeAttr attr= new EdgeAttr(attr_data[0],attr_data[1],Long.parseLong(values[4]),true,Long.parseLong(values[5]));   
                             getSubgraph().getSubgraphValue().InEdges.put(Sink, new HashMap<Long,EdgeAttr>());
                             getSubgraph().getSubgraphValue().InEdges.get(Sink).put(Source, attr);
                      }
                    }
                            
                    
                    
                            
            }
            if(!gcCalled){
            System.gc();
            System.runFinalization();
            }
            LOG.info("TIME ACCUMULATING INEDGES....Starting loading of heuristics");


            if(!gcCalled){
                    System.gc();
                    System.runFinalization();
                    gcCalled=true;
            }
			
     
			
			
		}
		
		if(getSuperstep()==2){
			try{
				System.out.println("STARTING SUPERSTEP 2");
				// TODO :Later add support for multiple instances
				
				subgraphHueristics.numVertices = new Double(getSubgraph().getLocalVertexCount());
				subgraphHueristics.numRemoteVertices = new Double(getSubgraph().getVertexCount() - getSubgraph().getLocalVertexCount());
				long edgecount=0;
				for(Object o:getSubgraph().getOutEdges()){
				  edgecount++;
				}
				subgraphHueristics.numEdges = new Double(edgecount);
				Iterator<String> v_it = vertexProperties.iterator();
				Iterator<String> e_it = edgeProperties.iterator();
				while(v_it.hasNext()){
					String prop = v_it.next();
					vertexProperties.add(prop);
					subgraphHueristics.vertexPredicateMap.put(prop, new HashMap<String, vertexPredicateStats>());
				}
				while(e_it.hasNext()){
					String prop = e_it.next();
					edgeProperties.add(prop);
					subgraphHueristics.edgePredicateMap.put(prop, new HashMap<String,edgePredicateStats>());
				}
				
				//extract vertex hueristics and avg edge degree
				for(IVertex<MapWritable, MapWritable, LongWritable, LongWritable> vertex:getSubgraph().getLocalVertices()){
					if(vertex.isRemote()){continue;}
					Iterator<String> it = vertexProperties.iterator();
					while(it.hasNext()){
						String prop = it.next();
						String value = vertex.getValue().get(prop).toString();
						if( !subgraphHueristics.vertexPredicateMap.get(prop).containsKey(value) ){
							subgraphHueristics.vertexPredicateMap.get(prop).put(value, new vertexPredicateStats());
						}
						
						subgraphHueristics.vertexPredicateMap.get(prop).get(value).numberMatchingPredicate += 1;
						long size=0;
						Iterator edgeIter=vertex.getOutEdges().iterator();
						while(edgeIter.hasNext()){
						  size++;
						}
						    
						subgraphHueristics.vertexPredicateMap.get(prop).get(value).numOutDegree += size;
						
						Iterator _it = vertex.getOutEdges().iterator();
						while(_it.hasNext()){
							IEdge<MapWritable,LongWritable,LongWritable> edge = (IEdge<MapWritable, LongWritable, LongWritable>) _it.next();
							IVertex<MapWritable, MapWritable, LongWritable, LongWritable> sink=getSubgraph().getVertexById(edge.getSinkVertexId());
							if (sink.isRemote())
								subgraphHueristics.vertexPredicateMap.get(prop).get(value).numRemoteOutDegree += 1;
						}
						 
						
						//TODO: add inDegree support()
						
						int count = 0;
						if(getSubgraph().getSubgraphValue().InEdges.containsKey(vertex.getVertexId().get()))
							count=getSubgraph().getSubgraphValue().InEdges.get(vertex.getVertexId().get()).size();
//						_it = vertex.inEdges().iterator();
//						while(_it.hasNext()){
//							_it.next();
//							count++;
//						}
						
						if ( getSubgraph().getSubgraphValue().InEdges.containsKey( vertex.getVertexId().get()) ) {
							long remoteInDegree=0;
						        HashMap<Long,EdgeAttr> h=getSubgraph().getSubgraphValue().InEdges.get(vertex.getVertexId().get());
						        for(EdgeAttr e:h.values()){
						          if(e.isRemote){
						            remoteInDegree++;
						          }
						        }
							subgraphHueristics.vertexPredicateMap.get(prop).get(value).numRemoteInDegree +=  remoteInDegree;
						}
						
						subgraphHueristics.vertexPredicateMap.get(prop).get(value).numInDegree += count;
						
					}
				}
				
				for(Map.Entry<String,HashMap<String,vertexPredicateStats>> entry:subgraphHueristics.vertexPredicateMap.entrySet()){
					for(Map.Entry<String, vertexPredicateStats> entry_inside: entry.getValue().entrySet()){
						entry_inside.getValue().probability = new BigDecimal(entry_inside.getValue().numberMatchingPredicate / subgraphHueristics.numVertices).setScale(8, BigDecimal.ROUND_HALF_UP).doubleValue();
						entry_inside.getValue().avgOutDegree = new BigDecimal(entry_inside.getValue().numOutDegree / entry_inside.getValue().numberMatchingPredicate).setScale(8, BigDecimal.ROUND_HALF_UP).doubleValue();
						entry_inside.getValue().avgInDegree = new BigDecimal(entry_inside.getValue().numInDegree / entry_inside.getValue().numberMatchingPredicate).setScale(8, BigDecimal.ROUND_HALF_UP).doubleValue();
						entry_inside.getValue().avgRemoteOutDegree = new BigDecimal(entry_inside.getValue().numRemoteOutDegree / entry_inside.getValue().numberMatchingPredicate).setScale(8, BigDecimal.ROUND_HALF_UP).doubleValue();
						entry_inside.getValue().avgRemoteInDegree = new BigDecimal(entry_inside.getValue().numRemoteInDegree / entry_inside.getValue().numberMatchingPredicate).setScale(8, BigDecimal.ROUND_HALF_UP).doubleValue();

					}
				}
				
				System.out.println("Extracting edge heuristics");				
				//extract edge hueristics
				for(IEdge<MapWritable, LongWritable, LongWritable> edge: getSubgraph().getOutEdges()){
					Iterator<String> it = edgeProperties.iterator();
					while(it.hasNext()){
						String prop = it.next();
						String value = edge.getValue().get(prop).toString();
						if( subgraphHueristics.edgePredicateMap.get(prop).containsKey(value) ){
							subgraphHueristics.edgePredicateMap.get(prop).get(value).numberMatchingPredicate += 1;
						}
						else{
							subgraphHueristics.edgePredicateMap.get(prop).put(value, new edgePredicateStats());
							subgraphHueristics.edgePredicateMap.get(prop).get(value).numberMatchingPredicate += 1;
						}
					}
				}
				for(Map.Entry<String,HashMap<String,edgePredicateStats>> entry:subgraphHueristics.edgePredicateMap.entrySet()){
					for(Map.Entry<String, edgePredicateStats> entry_inside: entry.getValue().entrySet()){
						entry_inside.getValue().probability = new BigDecimal(entry_inside.getValue().numberMatchingPredicate / subgraphHueristics.numEdges).setScale(8, BigDecimal.ROUND_HALF_UP).doubleValue();
					}
				}
								
				System.out.println("Writing heuristic object:" + heuristicsBasePath+String.valueOf(getSubgraph().getSubgraphId())+".ser");
				//store in file
				File file = new File(heuristicsBasePath+String.valueOf(getSubgraph().getSubgraphId())+".ser");
				if ( !file.exists() ) {
					
					file.getParentFile().mkdirs();
					file.createNewFile();
					
				}
				fos = new FileOutputStream(file); 
				oos = new ObjectOutputStream(fos);
				oos.writeObject(subgraphHueristics);
				oos.close();
				
			}catch(Exception e){e.printStackTrace();}
			
		}

//		if(  getSuperstep() == 3) {
//			try {
//				FileInputStream fis = new FileInputStream(heuristicsBasePath+String.valueOf(getSubgraph().getSubgraphId())+".ser"); 
//				ObjectInputStream ois = new ObjectInputStream(fis);
//				Hueristics hueristics = (Hueristics)ois.readObject();
//				ois.close();
//				
//				output(hueristics.numVertices.toString());
//				output(hueristics.numEdges.toString());
//				for(Map.Entry<String,HashMap<String,vertexPredicateStats>> entry:hueristics.vertexPredicateMap.entrySet()){
//					output(entry.getKey()); 
//					for(Map.Entry<String, vertexPredicateStats> entry_inside: entry.getValue().entrySet()){
//						output(entry_inside.getKey()+":"+
//								entry_inside.getValue().numberMatchingPredicate+":"+
//								entry_inside.getValue().probability+":"+
//								entry_inside.getValue().avgOutDegree+":"+
//								entry_inside.getValue().avgInDegree+":"+
//								entry_inside.getValue().avgRemoteOutDegree+":"+
//								entry_inside.getValue().avgRemoteInDegree+":"
//							  );
//					}
//				}
//				for(Map.Entry<String,HashMap<String,edgePredicateStats>> entry:hueristics.edgePredicateMap.entrySet()){
//					output(entry.getKey().toString()); 
//					for(Map.Entry<String, edgePredicateStats> entry_inside: entry.getValue().entrySet()){
//						output(entry_inside.getKey()+":"+entry_inside.getValue().probability+":"+entry_inside.getValue().numberMatchingPredicate);
//					}
//				}
//				
//			}catch(Exception e) {System.out.println("Error while reading back heuristics object");}
//		}
		
		if (getSuperstep()==3)
			voteToHalt();
		
	}
	


 
  @Override
  public void wrapup() {
    // TODO Auto-generated method stub
    
  }

}
