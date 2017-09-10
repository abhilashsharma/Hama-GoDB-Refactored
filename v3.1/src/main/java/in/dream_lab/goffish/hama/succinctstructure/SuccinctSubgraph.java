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
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

public class SuccinctSubgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> implements ISubgraph<S, V, E, I, J, K> {
    private SuccinctIndexedFileBuffer succinctIndexedVertexFileBuffer,succinctIndexedEdgeFileBuffer;
    public Map<Long, Long> remotevertexToSubgraph;
    public K subgraphId;
    S _value;
//    String vertexPath,edgePath;
    public SuccinctSubgraph(K subgraphId, String vPath,String ePath)
    {
//    	vertexPath=vPath;
//    	edgePath=ePath;
    	succinctIndexedVertexFileBuffer = new SuccinctIndexedFileBuffer(vPath, StorageMode.MEMORY_ONLY);
        succinctIndexedEdgeFileBuffer = new SuccinctIndexedFileBuffer(ePath, StorageMode.MEMORY_ONLY);
        this.subgraphId = subgraphId;
    }
    /*public static SuccinctSubgraph createSubgraph(K subgraphId, String vPath, String ePath)
    {
    	return new SuccinctSubgraph(subgraphId, vPath, ePath);
    }*/
    public SuccinctIndexedFileBuffer getVertexBuffer()
    {
        return succinctIndexedVertexFileBuffer;
    }
    public SuccinctIndexedFileBuffer getEdgeBuffer()
    {
        return succinctIndexedEdgeFileBuffer;
    }
    public void setMap(Map<Long, Long> v2s)
    {
        this.remotevertexToSubgraph = v2s;
    }
    public Map<Long, Long> getMap()
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

  
    public List<Long> getVertexByProp(String name, String value, char delim)
    {
    	long start = System.nanoTime();
    	List<Long> vid = new ArrayList<>();
    	int offset;
    	String record;
    	String[] tokens;
    	Integer[] recordID = succinctIndexedVertexFileBuffer.recordSearchIds(value.getBytes());
    	for (Integer rid : recordID)
    	{
    		offset = succinctIndexedVertexFileBuffer.getRecordOffset(rid);
    		record = succinctIndexedVertexFileBuffer.extractUntil(offset, delim);
    		
    		tokens=record.split("\\W");
    		for(int i=0;i<tokens.length;i++) {
    			vid.add(Long.parseLong(tokens[i]));
    		}
    	}
    	Log.info("Querying Time:" + (System.nanoTime() - start));
    	return vid;
    }
}
