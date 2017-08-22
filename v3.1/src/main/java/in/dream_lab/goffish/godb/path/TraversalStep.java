package in.dream_lab.goffish.godb.path;

import in.dream_lab.goffish.godb.Path;
import in.dream_lab.goffish.godb.PathWithDir;

public class TraversalStep {

	// root vertex where this traversal is starting
	long rootVertex;

	// subgraph containing the root vertex
	long rootSubgraph;

	// vertex that has been visited and its results added to resultset, but whose
	// children have to be traversed
	long targetVertex;
	
	//subgraph from which it got initiation to continue traversal
	long previousSubgraph;
	
	//starting depth in this subgraph
	int startDepth;
	
	//starting vertex of traversal in this subgraph
	long startVertex;
	
	// Number of edges that have been traversed to reach this targetVetex.
	// 0 for the root, 1 for its children, etc.
	int depth;

	public TraversalStep(long sg_, long r_, long t_, int d_) {
		rootSubgraph = sg_;
		rootVertex = r_;
		targetVertex = t_;
		depth = d_;
	}
	
	
	public TraversalStep(long sg_, long r_, long t_,long pg_,int sd_,long sv_, int d_) {
          rootSubgraph = sg_;
          rootVertex = r_;
          targetVertex = t_;
          depth = d_;
          previousSubgraph=pg_;
          startDepth=sd_;
          startVertex=sv_;
    }

	@Override
	public String toString() {
		return new StringBuilder("TraversalStep,RootSG,").append(rootSubgraph).append(",RootVertex,").append(rootVertex)
		        .append(",TargetVertex,").append(targetVertex).append(",depth,").append(depth).toString();
	}

	public static class WithResults extends TraversalStep {
		public WithResults(long sg_, long r_, long s_, long e_, long t_, int d_) {
			super(sg_, r_, t_, d_);
			sourceVertex = s_;
			edge = e_;
		}

		long sourceVertex, edge;
	}
	
	public static class TraversalWithPath extends TraversalStep {
	  
	  Path path;
	        public TraversalWithPath(long sg_, long r_, long t_, int d_, Path p_){
	                super(sg_, r_, t_, d_);              
	                path=p_;
	        }
	        public TraversalWithPath(long sg_, long r_, long t_,long pg_,int sd_,long sv_,  int d_, Path p_){
                  super(sg_, r_, t_,pg_,sd_,sv_, d_);              
                  path=p_;
          }
	        
	        
	}
	
	
public static class TraversalWithState extends TraversalStep {
          
          PathWithDir path;
          long queryId;
                public TraversalWithState(long qid_,long sg_, long r_, long t_, int d_, PathWithDir p_){
                        super(sg_, r_, t_, d_);              
                        path=p_;
                        queryId=qid_;
                }
                public TraversalWithState(long qid_,long sg_, long r_, long t_,long pg_,int sd_,long sv_,  int d_, PathWithDir p_){
                  super(sg_, r_, t_,pg_,sd_,sv_, d_);              
                  path=p_;
                  queryId=qid_;
          }
                
                
        }
	
	
	
}