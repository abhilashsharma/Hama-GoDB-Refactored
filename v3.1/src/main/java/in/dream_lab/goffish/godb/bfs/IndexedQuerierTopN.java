package in.dream_lab.goffish.godb.bfs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;

import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.godb.ConfigFile;
import in.dream_lab.goffish.godb.MapValue;
import in.dream_lab.goffish.godb.BFSDistr.VertexMessageSteps;


public class IndexedQuerierTopN implements IBFSRootQuerier {

	///////////////////////////////////////
	// STATIC INDEX RELATED FIELDS
	//
	// Common variable to indicate if for this partition/worker, the index state
	// has been loaded by one of the subgraphs.
	private static boolean indexInitialized = false;
	private static final Object INDEX_WRITE_LOCK = new Object();

	// Common variable to indicate if for this partition/worker, the index
	// has been queried for the source vertices
	private static boolean queryMade = false;
	private static final Object QUERY_LOCK = new Object();


	// Index searchers and root vertex hits across subgraphs in this worker
	private static IndexSearcher indexSearcher;
	private static int hitCount;
	// Map from subgraph ID in local worker to the list of root vertices in that
	// SG
	private static Map<Long, List<Long>> rootVertexWorkerMap;


	IndexedQuerierTopN() {}

	/**
	 * Initialize Lucene in memory.
	 * searcher = new IndexSearcher (new RAMDirectory (indexDirectory));
	 * Only one of the subgraphs in this worker performs this task.
	 * 
	 */
	public boolean loadIndex(long sgid) {
		// Load index once per partition/worker from among multiple subgraphs
		try {
			synchronized (INDEX_WRITE_LOCK) {
				if (!indexInitialized) {
					// FIXME: this is not guaranteed to be unique or deterministic!
					// Use the full SGID for path without shifting
					long pseudoPid = sgid >> 32;
					File vertexIndexDir = new File(ConfigFile.basePath + "/index/Partition" + pseudoPid + "/vertexIndex");
					FSDirectory vertexDirectory = FSDirectory.open(vertexIndexDir);
					// passing RAM directory to load indexes in memory
					DirectoryReader indexReader = DirectoryReader.open(new RAMDirectory(vertexDirectory, IOContext.READ));
					indexSearcher = new IndexSearcher(indexReader);
					indexInitialized = true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return indexInitialized;
	}


	/**
	 * This queries for the root vertices in this worker using the lucene index.
	 * This query is done once per worker, across all subgraphs.
	 * 
	 * The first subgraph to reach this method populates the rootVertexWorkerMap
	 * with the list of root vertices present in each subgraph of this worker.
	 * It then removes and returns the list of vertex IDs that match this
	 * particular subgraph from the Map.
	 * 
	 * Calls by future subgraphs will just return the list from this pre-populated
	 * Map.
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<Long> queryRootVertices(
	        ISubgraph<BFSState, MapValue, MapValue, LongWritable, LongWritable, LongWritable> subgraph)
	        throws IOException {

		long sgid = subgraph.getSubgraphId().get();
		BFSQuery query = subgraph.getSubgraphValue().query;

		// do indexed query
		if (!indexInitialized) throw new RuntimeException("Index has not been initialialized!");
		synchronized (QUERY_LOCK) {
			if (!queryMade) {
				rootVertexWorkerMap = queryRootVerticesIndexed(query.getPropertyName(), query.getPropertyValue());
//				System.out.println("WORKERMAP:"+ rootVertexWorkerMap.size());
				queryMade = true;
			}
			// return and delete matches for each root vertex from static result map
//			System.out.println("REMOVINGSGID:" + sgid);
			return rootVertexWorkerMap.remove(sgid);
		}
	}


	/**
	 * One subgraph searches Lucene Index for the boolean match and returns
	 * a Map with the root vertex IDs for each subgraph ID in this worker
	 * 
	 * @param prop
	 * @param val
	 * @return
	 * @throws IOException
	 */
	private static Map<Long, List<Long>> queryRootVerticesIndexed(String prop, Object val) throws IOException {

		Query query;
		if (val instanceof String) {
//		        System.out.println("Querying:"+ prop + "," + val.toString());
			query = new BooleanQuery();
			((BooleanQuery) query).add(new TermQuery(new Term(prop, (String) val)), BooleanClause.Occur.MUST);
		} else if (val instanceof Integer) {
			query = NumericRangeQuery.newIntRange(prop, (Integer) val, (Integer) val, true, true);
		} else
			throw new RuntimeException("Unsupported query value type: " + val);

		hitCount = 0;
		final Map<Long, List<Long>> rootVertexMap = new HashMap<>();

		ScoreDoc[] hits = indexSearcher.search(query,40000).scoreDocs;
		
		if(hits.length>0){
                  for (int i=0;i<hits.length;i++){
                          Document doc = indexSearcher.doc(hits[i].doc);
                          long sgid = Long.parseLong(doc.get("subgraphid"));
                          long rootVertex = Long.parseLong(doc.get("id"));
                          
                          List<Long> rootList = rootVertexMap.get(sgid);
                          if (rootList == null) {
                                  rootList = new ArrayList<>();
//                                  System.out.println("ENTERSGID:" + sgid + "," +  rootVertex);
                                  rootVertexMap.put(sgid, rootList);
                          }
                          rootList.add(rootVertex);

                          hitCount++;
                            
                  }
                }
		
		
		return rootVertexMap;
	}


	public void clear() {
		queryMade = false;
		rootVertexWorkerMap = null;
		hitCount = -1;
	}

	static int getHitCount() {
		return hitCount;
	}
}
