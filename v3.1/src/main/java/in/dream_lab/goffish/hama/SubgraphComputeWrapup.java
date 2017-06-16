package in.dream_lab.goffish.hama;

import java.util.Collection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IMessage;


//change class name to subgraphcomputewrapup
public abstract class SubgraphComputeWrapup <S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable> 
extends SubgraphCompute <S, V, E, M, I, J, K> {
	
	
	//change finalize to wrapup()
	public abstract void wrapup();


}
