package in.dream_lab.goffish.godb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.TreeMap;



//used for histograms.... comment it out when using exact statistics
		public class HueristicsTreeMap implements Serializable{
			private static final long serialVersionUID = 1L;
			public Double numVertices=new Double(0);
			public Double numRemoteVertices=new Double(0);
			public Double numEdges=new Double(0);
			public HashMap<String,TreeMap<String,vertexPredicateStats>> vertexPredicateMap;
			public HashMap<String,TreeMap<String,edgePredicateStats>> edgePredicateMap;
			
			public HueristicsTreeMap(){
				vertexPredicateMap = new HashMap<String,TreeMap<String,vertexPredicateStats>>();
				edgePredicateMap = new HashMap<String,TreeMap<String,edgePredicateStats>>();
			}
		}