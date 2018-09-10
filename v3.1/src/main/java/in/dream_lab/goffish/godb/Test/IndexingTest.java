package in.dream_lab.goffish.godb.Test;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import in.dream_lab.goffish.godb.ConfigFile;

public class IndexingTest {

	static File vertexIndexDir;
	static Directory vertexDirectory;
	static Analyzer analyzer;
	static IndexReader indexReader;
	static IndexSearcher indexSearcher;
	static BooleanQuery query;
	static ScoreDoc[] hits;
	
	
	public static void main(String[] args) throws InterruptedException, IOException {
		for (long i=0;i<16;i++) {
		initInMemoryLucene(i);
		String currentProperty="employer";
		String currentValue="atodahora";
		makeQuery(currentProperty,currentValue);
		
		System.out.println("Index "+ i+ ": " + hits.length);
		}
		
		
	}
	
	//for querying lucene for starting vertex
			private static  void makeQuery(String prop,Object val) throws IOException{
				{
//					queryMade = true;
					if(val.getClass()==String.class){
					query  = new BooleanQuery();
					query.add(new TermQuery(new Term(prop, (String)val)), BooleanClause.Occur.MUST);
					hits =  indexSearcher.search(query,40000).scoreDocs;
					}
					else if(val.getClass()==Integer.class)
					{
						Query q = NumericRangeQuery.newIntRange(prop,(Integer)val, (Integer)val, true, true);
						hits =  indexSearcher.search(q,40000).scoreDocs;
					}
					
				}
			}
	
	private static void initInMemoryLucene(long pseudoPid) throws InterruptedException, IOException{
        {
//          long pseudoPid=getSubgraph().getSubgraphId().get() >> 32;
//                initDone = true;
                vertexIndexDir = new File(ConfigFile.basePath+ "/index/Partition"+pseudoPid+"/vertexIndex");
                vertexDirectory = FSDirectory.open(vertexIndexDir);
                analyzer = new StandardAnalyzer(Version.LATEST);
                indexReader  = DirectoryReader.open(new RAMDirectory(vertexDirectory, IOContext.READ));//passing RAM directory to load indexes in memory
                indexSearcher = new IndexSearcher(indexReader);
        }
     
     
   }
	
	
}
