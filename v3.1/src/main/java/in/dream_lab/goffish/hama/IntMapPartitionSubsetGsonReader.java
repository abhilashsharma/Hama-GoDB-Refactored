package in.dream_lab.goffish.hama;


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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;
import com.google.gson.*;


import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.godb.MapValue;
import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;
import in.dream_lab.goffish.hama.utils.DisjointSets;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;

/*
 * 
 * First Line contains PseudoPid
 * [srcid, srcvalue, [[sinkid1,edgeid1,edgevalue1], [sinkid2,edgeid2,edgevalue2] ... ]] 
 * PseudoPid is considered for all vertices in file
 * 
 * This Reader only subset of the properties
 */

public class IntMapPartitionSubsetGsonReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, IntWritable, IntWritable, LongWritable> {

  HamaConfiguration conf;
  Map<IntWritable, IVertex<V, E, IntWritable, IntWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  private Map<IntWritable, LongWritable> vertexSubgraphMap;
  private Set<String> vertexPropertySet= new HashSet<String>();
  private Set<String> edgePropertySet=new HashSet<String>();
  
  JsonParser GsonParser = new JsonParser();
  public IntMapPartitionSubsetGsonReader(
              BSPPeerImpl<Writable, Writable, Writable, Writable, Message<K, M>> peer,
              HashMap<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.conf = peer.getConfiguration();
    this.vertexSubgraphMap = new HashMap<IntWritable, LongWritable>();
    this.vertexPropertySet.add("patid");
//    this.vertexPropertySet.add("nclass");
//    this.vertexPropertySet.add("country");
  }
  
  public static final Log LOG = LogFactory.getLog(IntMapPartitionSubsetGsonReader.class);
  Integer pseudoPartId=null;
  @Override
  public List<ISubgraph<S, V, E, IntWritable, IntWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {
          LOG.info("Creating vertices");

    vertexMap = new HashMap<IntWritable, IVertex<V, E, IntWritable, IntWritable>>();

    // List of edges.Used to create RemoteVertices
    //List<IEdge<E, IntWritable, IntWritable>> _edges = new ArrayList<IEdge<E, IntWritable, IntWritable>>();
    
    KeyValuePair<Writable, Writable> pair;
    pair=peer.readNext();
    pseudoPartId=Integer.parseInt(pair.getValue().toString());
    while ((pair = peer.readNext()) != null) {
      String StringJSONInput = pair.getValue().toString();
      
      
        Vertex<V, E, IntWritable, IntWritable> vertex = createVertex(
            StringJSONInput);
        vertexMap.put(vertex.getVertexId(), vertex);
        //_edges.addAll(vertex.getOutEdges());
      }
    
    //LOG.info("Sending Vertices to respective partitions");

    //LOG.info("Received all vertices");

    List<IVertex<V, E, IntWritable, IntWritable>> vertices = Lists
        .newArrayList();
    for (IVertex<V, E, IntWritable, IntWritable> vertex : vertexMap
        .values()) {
      vertices.add(vertex);
    }
    //System.out.println("Vertices size =" + vertices.size());

    /* Create remote vertex objects. */
//    int remoteVertices = 0;
    for (IVertex<V, E, IntWritable, IntWritable> vertex : vertices) {
      for (IEdge<E, IntWritable, IntWritable> e : vertex.getOutEdges()) {
        IntWritable sinkID = e.getSinkVertexId();
        IVertex<V, E, IntWritable, IntWritable> sink = vertexMap.get(sinkID);
        if (sink == null) {
          sink = new RemoteVertex<V, E, IntWritable, IntWritable, IntWritable>(
              sinkID);
          vertexMap.put(sinkID, (IRemoteVertex<V, E, IntWritable, IntWritable,IntWritable>)sink);
//          remoteVertices++;
        }
      }
    }
//    int local = 0,remote =0;
//    for (IVertex<V, E, IntWritable, IntWritable> vertex : vertexMap.values()) {
//      if (!vertex.isRemote()) {
//        local ++;
//      }
//      else
//        remote++;
//    }
//    System.out.println("Local = "+local +" Remote = "+remote);
    /*
    for (IEdge<E, IntWritable, IntWritable> e : _edges) {
      IntWritable sinkID = e.getSinkVertexId();
      IVertex<V, E, IntWritable, IntWritable> sink =  vertexMap.get(sinkID);
      if (sink == null) {
        sink = new RemoteVertex<V, E, IntWritable, IntWritable, IntWritable>(sinkID);
        vertexMap.put(sinkID, sink);
      }
    }*/
    
    //Direct Copy paste from here
    //TODO: pass  pid instead of peerindex
    
    Partition<S, V, E, IntWritable, IntWritable, LongWritable> partition = new Partition<S, V, E, IntWritable, IntWritable, LongWritable>(peer.getPeerIndex());
    
    formSubgraphs(partition, vertexMap.values());
    
    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    Message<IntWritable, IntWritable> question = new Message<IntWritable, IntWritable>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    question.setControlInfo(controlInfo);
    /*
     * Message format being sent:
     * partitionID remotevertex1 remotevertex2 ...
     */
//    int n=0;
    byte partitionIDbytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionIDbytes);
    for (IVertex<V, E, IntWritable, IntWritable> v : vertexMap.values()) {
      if (v instanceof RemoteVertex) {
//        n++;
        byte vertexIDbytes[] = Ints.toByteArray(v.getVertexId().get());
        controlInfo.addextraInfo(vertexIDbytes);
      }
    }
    
//    if (n != remoteVertices) {
//      System.out.println("Something wrong with remote vertex broadcast");
//    }
    sendToAllPartitions(question);

    peer.sync();
    
    Message<IntWritable, IntWritable> msg;
    //Receiving 1 message per partition
    while ((msg = (Message<IntWritable, IntWritable>) peer.getCurrentMessage()) != null) {
      /*
       * Subgraph Partition mapping broadcast
       * Format of received message:
       * partitionID subgraphID1 subgraphID2 ...
       */
      if (msg.getMessageType() == Message.MessageType.SUBGRAPH) {
        Iterable<BytesWritable> subgraphList = ((ControlMessage) msg
            .getControlInfo()).getExtraInfo();
        
        Integer partitionID = Ints.fromByteArray(subgraphList.iterator().next().getBytes());
        
        for (BytesWritable subgraphListElement : Iterables.skip(subgraphList,1)) {
          LongWritable subgraphID = new LongWritable(
              Longs.fromByteArray(subgraphListElement.getBytes()));
          subgraphPartitionMap.put((K) subgraphID, partitionID);
        }
        continue;
      }
      
      /*
       * receiving query to find subgraph id Remote Vertex
       */
      Iterable<BytesWritable> RemoteVertexQuery = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();
      
      /*
       * Reply format :
       * sinkID1 subgraphID1 sinkID2 subgraphID2 ...
       */
      Message<IntWritable, IntWritable> subgraphIDReply = new Message<IntWritable, IntWritable>(); 
      controlInfo = new ControlMessage();
      controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
      subgraphIDReply.setControlInfo(controlInfo);
      
      Integer sinkPartition = Ints.fromByteArray(RemoteVertexQuery.iterator().next().getBytes());
      boolean hasAVertex = false;
      for (BytesWritable remoteVertex : Iterables.skip(RemoteVertexQuery,1)) {
        IntWritable sinkID = new IntWritable(Ints.fromByteArray(remoteVertex.getBytes()));
        LongWritable sinkSubgraphID = vertexSubgraphMap.get(sinkID);
        //In case this partition does not have the vertex 
        /* Case 1 : If vertex does not exist
         * Case 2 : If vertex exists but is remote, then its subgraphID is null
         */
        if (sinkSubgraphID == null) {
          continue;
        }
        hasAVertex = true;
        byte sinkIDbytes[] = Ints.toByteArray(sinkID.get());
        controlInfo.addextraInfo(sinkIDbytes);
        byte subgraphIDbytes[] = Longs.toByteArray(sinkSubgraphID.get());
        controlInfo.addextraInfo(subgraphIDbytes);
      }
      if (hasAVertex) {
        peer.send(peer.getPeerName(sinkPartition.intValue()),
            (Message<K, M>) subgraphIDReply);
      }
    }
    peer.sync();
    
    int remoteV = 0;
    while ((msg = (Message<IntWritable, IntWritable>)peer.getCurrentMessage()) != null) {
      Iterable<BytesWritable> remoteVertexReply = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();
      
      Iterator<BytesWritable> queryResponse = remoteVertexReply.iterator();
      
      while(queryResponse.hasNext()) {
        IntWritable sinkID = new IntWritable(Ints.fromByteArray(queryResponse.next().getBytes()));
        LongWritable remoteSubgraphID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        RemoteVertex<V, E, IntWritable, IntWritable, LongWritable> sink =(RemoteVertex<V, E, IntWritable, IntWritable, LongWritable>) vertexMap.get(sinkID);
        if (sink == null) {
          System.out.println("NULL"+sink);
        }
        remoteV++;
//        System.out.println("Setting subgraph id for remote vertex");
        sink.setSubgraphID(remoteSubgraphID);
//        sink.getSubgraphId();
      }
    }
    
//    if(remoteV != remoteVertices) {
//      System.out.println("Skipped a few RemoteVertices "+(remoteVertices - remoteV));
//    }
    
    return partition.getSubgraphs();
  }
  
  /* takes partition and message list as argument and sends the messages to their respective partition.
   * Needed to send messages just before peer.sync(),as a hama bug causes the program to stall while trying
   * to send and recieve(iterate over recieved message) large messages at the same time
   */
  private void sendMessage(int partition,
      List<Message<IntWritable, IntWritable>> messageList) throws IOException {
    
    for (Message<IntWritable, IntWritable> message : messageList) {
      peer.send(peer.getPeerName(partition), (Message<K, M>)message);
    }
    
  }
  
  private void sendToAllPartitions(Message<IntWritable, IntWritable> message) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, (Message<K, M>) message);
    }
  }
  
  @SuppressWarnings("unchecked")
  Vertex<V, E, IntWritable, IntWritable> createVertex(String JSONString) {
    JsonArray JSONInput = GsonParser.parse(JSONString).getAsJsonArray();

    IntWritable sourceID = new IntWritable(
        Integer.valueOf(JSONInput.get(0).toString()));
    assert (vertexMap.get(sourceID) == null);
    
    
    //fix this
  //assumed value of jsonMap= "key1:type1:value1$ key2:type2:value2$....."
    //type could be Long or String or Double
    String jsonMap=JSONInput.get(1).toString();
    jsonMap=jsonMap.substring(1, jsonMap.length()-1);
//    LOG.info("JSONMAP:" + jsonMap);
    String[] vprop=jsonMap.split(Pattern.quote("$"));
    //key,value property pairs for a vertex
    MapValue vertexValueMap=new MapValue();
    for(int i=0;i<vprop.length;i++){
        String[] map=vprop[i].split(Pattern.quote(":"));
          
//        LOG.info("HashMap:" + map[0] + "," + map[2]);
        if(vertexPropertySet.contains(map[0])){
          
            vertexValueMap.put(map[0], map[2]);
//            LOG.info("Entered HashMap:" + map[0] + "," + map[2]);
        }
        
    }
    
    V vertexValue = (V) vertexValueMap;
    
    
    List<IEdge<E, IntWritable, IntWritable>> _adjList = new ArrayList<IEdge<E, IntWritable, IntWritable>>();
    JsonArray edgeList = (JsonArray) JSONInput.get(2);
    for (Object edgeInfo : edgeList) {
      JsonArray edgeValues = ((JsonArray) edgeInfo).getAsJsonArray();
      IntWritable sinkID = new IntWritable(
          Integer.valueOf(edgeValues.get(0).toString()));
      IntWritable edgeID = new IntWritable(
          Integer.valueOf(edgeValues.get(1).toString()));
      //fix this
      //same format as vertex
      String[] eprop= edgeValues.get(2).toString().split(Pattern.quote("$"));
//      MapWritable edgeMap=new MapWritable();
//      for(int i=0;i<eprop.length;i++){
//        String[] map=eprop[i].split(Pattern.quote(":"));
//        Text key=null;
//        Text value=null;
//        if(edgePropertySet.contains(map[0])){
//           key=new Text(map[0]);
//        //FIXME:assuming String values for now
//           value=new Text(map[2]);
//           edgeMap.put(key, value);
//        }
        
//      }
      
      Edge<E, IntWritable, IntWritable> edge = new Edge<E, IntWritable, IntWritable>(
          edgeID, sinkID);
//      E edgeValue = (E) edgeMap;
      edge.setValue(null);
      _adjList.add(edge);
    }
    Vertex<V, E, IntWritable, IntWritable> vertex = new Vertex<V, E, IntWritable, IntWritable>(
        sourceID,_adjList);
    vertex.setValue(vertexValue);
    return vertex;
  }
  
  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(Partition<S, V, E, IntWritable, IntWritable, LongWritable> partition, Collection<IVertex<V, E, IntWritable, IntWritable>> vertices) throws IOException {
    
    long subgraphCount = 0;
    Message<IntWritable, IntWritable> subgraphLocationBroadcast = new Message<IntWritable, IntWritable>();
    
    subgraphLocationBroadcast.setMessageType(Message.MessageType.SUBGRAPH);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo
        .setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    subgraphLocationBroadcast.setControlInfo(controlInfo);
    
    byte partitionBytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionBytes);
    
        // initialize disjoint set
    DisjointSets<IVertex<V, E, IntWritable, IntWritable>> ds = new DisjointSets<IVertex<V, E, IntWritable, IntWritable>>(
        vertices.size());
    for (IVertex<V, E, IntWritable, IntWritable> vertex : vertices) {
      ds.addSet(vertex);
    }

    // union edge pairs
    for (IVertex<V, E, IntWritable, IntWritable> vertex : vertices) {
      if (!vertex.isRemote()) {
        for (IEdge<E, IntWritable, IntWritable> edge : vertex.getOutEdges()) {
          IVertex<V, E, IntWritable, IntWritable> sink = vertexMap
              .get(edge.getSinkVertexId());
          ds.union(vertex, sink);
        }
      }
    }

    Collection<? extends Collection<IVertex<V, E, IntWritable, IntWritable>>> components = ds
        .retrieveSets();

    for (Collection<IVertex<V, E, IntWritable, IntWritable>> component : components) {
      LongWritable subgraphID = new LongWritable(
          subgraphCount++ | (((long) pseudoPartId) << 32));
      Subgraph<S, V, E, IntWritable, IntWritable, LongWritable> subgraph = new Subgraph<S, V, E, IntWritable, IntWritable, LongWritable>(
          peer.getPeerIndex(), subgraphID);
      

      for (IVertex<V, E, IntWritable, IntWritable> vertex : component) {
        subgraph.addVertex(vertex);
        
        // Dont add remote vertices to the VertexSubgraphMap as remote vertex subgraphID is unknown
        if (!vertex.isRemote()) {
          vertexSubgraphMap.put(vertex.getVertexId(), subgraph.getSubgraphId());
        }        
      }
      
      partition.addSubgraph(subgraph);
      
      byte subgraphIDbytes[] = Longs.toByteArray(subgraphID.get());
      controlInfo.addextraInfo(subgraphIDbytes); 
     
    }
    sendToAllPartitions(subgraphLocationBroadcast);
  }
}
