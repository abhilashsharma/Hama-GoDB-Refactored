package in.dream_lab.goffish.hama.succinctstructure;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

public class SuccinctArraySubgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> implements ISubgraph<S, V, E, I, J, K> {
  private SuccinctIndexedFileBuffer succinctIndexedVertexFileBuffer,succinctIndexedEdgeFileBuffer;
    public Map<Long, Long> remotevertexToSubgraph;
    public Map<Long, Long> localvertexToSubgraph;
    public K subgraphId;
    S _value;
    public static final org.apache.commons.logging.Log Log = LogFactory.getLog(SuccinctArraySubgraph.class);
    List<SuccinctIndexedFileBuffer> vertexSuccinctBufferList;
    List<SuccinctIndexedFileBuffer> edgeSuccinctBufferList;
//    String vertexPath,edgePath;
    public SuccinctArraySubgraph(K subgraphId, List<SuccinctIndexedFileBuffer> _vertexSuccinctBufferList,List<SuccinctIndexedFileBuffer> _edgeSuccinctBufferList)
    {
//    	vertexPath=vPath;
//    	edgePath=ePath;
    	vertexSuccinctBufferList=_vertexSuccinctBufferList;
    	edgeSuccinctBufferList=_edgeSuccinctBufferList;
        this.subgraphId = subgraphId;
    }
    
    public SuccinctIndexedFileBuffer getVertexBuffer(int index)
    {
        return vertexSuccinctBufferList.get(index);
    }
    public SuccinctIndexedFileBuffer getEdgeBuffer(int index)
    {
        return edgeSuccinctBufferList.get(index);
    }
    
    public List<SuccinctIndexedFileBuffer> getVertexBufferList()
    {
        return vertexSuccinctBufferList;
    }
    public List<SuccinctIndexedFileBuffer> getEdgeBufferList()
    {
        return edgeSuccinctBufferList;
    }
    
    public void setRemoteMap(Map<Long, Long> v2s)
    {
        this.remotevertexToSubgraph = v2s;
    }
    
    public Map<Long, Long> getRemoteMap()
    {
        return remotevertexToSubgraph;
    }
   
    public K getSubgraphId()
    {
        return subgraphId;
    }
    public IVertex<V, E, I, J> getVertexById(I vertexId)
    {
        throw new UnsupportedOperationException("Function not supported");
    }
    public long getVertexCount()
    {
        throw new UnsupportedOperationException("We do not count the number of vertices");
    }
    public long getLocalVertexCount()
    {
        throw new UnsupportedOperationException("We do not count the number of local vertices");
    }
    public Iterable<IVertex<V, E, I, J>> getVertices()
    {
        throw new UnsupportedOperationException("We do not get the list of vertices from a subgraph");
    }
    public Iterable<IVertex<V, E, I, J>> getLocalVertices()
    {
        throw new UnsupportedOperationException("We do not get the list of local vertices");
    }
    public Iterable<IRemoteVertex<V, E, I, J, K>> getRemoteVertices()
    {
        throw new UnsupportedOperationException("We do not get the list of remote vertices");
    }
    public Iterable<IEdge<E, I, J>> getOutEdges()
    {
        throw new UnsupportedOperationException("We do not get the list of out edges");
    }
    public IEdge<E, I, J> getEdgeById(J edgeID)
    {
        throw new UnsupportedOperationException("We do not get an edge by its id");
    }
    @Override
    public void setSubgraphValue(S value) {
      _value = value;
    }

    @Override
    public S getSubgraphValue() {
     
	return _value;
    }
    //TODO:Change This..DONE
    public List<Long> getVertexIDs()
    {
    	
    	List<Long> vertices = new ArrayList<>();
    	for(SuccinctIndexedFileBuffer succinctIndexedVertexFileBuffer:vertexSuccinctBufferList) {
	    	long size = succinctIndexedVertexFileBuffer.count("#".getBytes());
	    	Integer offset;
	    	String v;
	    	long start;
	    	Log.info("getVertxIDs");
	    	for (int i = 0; i < size; i++) {
	    	    start = System.nanoTime();	
	    		offset = succinctIndexedVertexFileBuffer.getRecordOffset(i);
	    		Log.info("Lookup record offset(vertex): "+ (System.nanoTime() - start) + " ns\n"+"Size: "+offset.toString().length());
	    		start = System.nanoTime();
	    		v = succinctIndexedVertexFileBuffer.extractUntil(offset, '@');
	    		Log.info("VFORMAT:" +v);
	    		Log.info("Extract until(vertex): "+(System.nanoTime() - start) + " ns");
	    		vertices.add(Long.parseLong(v.split("#")[1]));
	    	}
    	}
    	return vertices;	
    }
  
    //TODO:Change this..DONE
    public List<Long> getVertexByProp(String name, String value, char delim)
    {
    	long start = System.nanoTime();
    	List<Long> vid = new ArrayList<>();
    	Log.info("getVertexByProp");
    	for(SuccinctIndexedFileBuffer succinctIndexedVertexFileBuffer:vertexSuccinctBufferList) {
    	Integer offset;
    	String record;
    	String[] tokens;
    	long startFine = System.nanoTime();
    	Integer[] recordID = succinctIndexedVertexFileBuffer.recordSearchIds(value.getBytes());
    	Log.info("Lookup record id(vertex): "+ (System.nanoTime() - startFine)+ " ns " + recordID.length);
    	for (Integer rid : recordID)
    	 {
    		startFine = System.nanoTime();
    		offset = succinctIndexedVertexFileBuffer.getRecordOffset(rid);
    		Log.info("Lookup record offset(vertex): " + (System.nanoTime() - startFine) + " ns");
    		startFine = System.nanoTime();
    		record = succinctIndexedVertexFileBuffer.extractUntil(offset, delim);
    		Log.info("Extract until(vertex): "+(System.nanoTime() - startFine) + " ns");
    		Log.info("# Extracted Bytes: " + record.length());
    		tokens=record.split("\\W");
    		for(int i=0;i<tokens.length;i++) {
    			vid.add(Long.parseLong(tokens[i]));
    		}
    	}
    	
    	}
    	Log.info("Querying Time:" + (System.nanoTime() - start));
    	return vid;
    }
}
