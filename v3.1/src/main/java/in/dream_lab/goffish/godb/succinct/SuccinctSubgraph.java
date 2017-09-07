package in.dream_lab.goffish.godb.succinct;
import java.util.ArrayList;
import java.util.List;


import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
public class SuccinctSubgraph {
    private SuccinctIndexedFileBuffer succinctIndexedFileBuffer;
    private SuccinctSubgraph(String path)
    {
    	succinctIndexedFileBuffer = new SuccinctIndexedFileBuffer(path, StorageMode.MEMORY_ONLY);
    }
    public static SuccinctSubgraph createSubgraph(String path)
    {
    	return new SuccinctSubgraph(path);
    }
    public List<Long> getVertices(String name, String value,char delim)
    {
//    	System.out.println("Calling getVertices");
    	List<Long> vid = new ArrayList<>();
    	int offset;
    	String record;
    	String[] tokens;
    	Integer[] recordID = succinctIndexedFileBuffer.recordSearchIds(value.getBytes());
//    	System.out.println("RecordId returned:"+ recordID[0]);
    	for (Integer rid : recordID)
    	{
    		offset = succinctIndexedFileBuffer.getRecordOffset(rid); 
    		record=succinctIndexedFileBuffer.extractUntil(offset, delim);
    		
    		tokens=record.split("\\W");
    		for(int i=0;i<tokens.length;i++) {
    			vid.add(Long.parseLong(tokens[i]));
    		}
//    		System.out.println("Offset:" + offset);
    	}
    	return vid;
    }
}
