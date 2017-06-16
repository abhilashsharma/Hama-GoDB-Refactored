package in.dream_lab.goffish.godb.reach;

public class TraversalStep {

	// root vertex where this traversal is starting
	long rootVertex;

	// subgraph containing the root vertex
	long rootSubgraph;

	// vertex that has been visited and its results added to resultset, but whose
	// children have to be traversed
	long targetVertex;

	// Number of edges that have been traversed to reach this targetVetex.
	// 0 for the root, 1 for its children, etc.
	int depth;

	public TraversalStep(long sg_, long r_, long t_, int d_) {
		rootSubgraph = sg_;
		rootVertex = r_;
		targetVertex = t_;
		depth = d_;
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
	  
	        public TraversalWithPath(long sg_, long r_, long t_, int d_, Path p_){
	                super(sg_, r_, t_, d_);              
	                path=p_;
	        }
	        Path path;
	}
	
}