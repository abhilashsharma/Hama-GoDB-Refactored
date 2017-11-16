/*     */ package in.dream_lab.goffish.godb;
/*     */ 



import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.api.IVertex;

/*     */ import java.io.File;
/*     */ import java.io.FileOutputStream;
/*     */ import java.io.FileWriter;
/*     */ import java.io.IOException;
/*     */ import java.io.ObjectOutputStream;
/*     */ import java.io.PrintWriter;
/*     */ import java.util.ArrayList;
/*     */ import java.util.Arrays;
import java.util.Collection;
/*     */ import java.util.HashMap;
/*     */ import java.util.HashSet;
/*     */ import java.util.Iterator;
/*     */ import java.util.List;
import java.util.Map;
/*     */ import java.util.Map.Entry;
/*     */ import java.util.Set;
/*     */ import java.util.concurrent.Semaphore;
/*     */ import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
/*     */ import org.apache.lucene.analysis.Analyzer;
/*     */ import org.apache.lucene.analysis.standard.StandardAnalyzer;
/*     */ import org.apache.lucene.document.Document;
/*     */ import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
/*     */ import org.apache.lucene.document.Field.Store;
/*     */ import org.apache.lucene.document.FloatField;
/*     */ import org.apache.lucene.document.IntField;
/*     */ import org.apache.lucene.document.LongField;
/*     */ import org.apache.lucene.document.StringField;
/*     */ import org.apache.lucene.index.IndexWriter;
/*     */ import org.apache.lucene.index.IndexWriterConfig;
/*     */ import org.apache.lucene.index.IndexWriterConfig.OpenMode;
/*     */ import org.apache.lucene.store.Directory;
/*     */ import org.apache.lucene.store.FSDirectory;
/*     */ import org.apache.lucene.util.Version;
/*     */ 
/*     */ 
/*     */ public class goffishIndexBuilder extends
/*     */    AbstractSubgraphComputation<LongWritable, MapValue, MapValue, Text, LongWritable, LongWritable, LongWritable>
/*     */ implements ISubgraphWrapup{
  
  
  public goffishIndexBuilder(String initMsg){
    Argument=initMsg;
  }
            String Argument=null;
            public static final Log LOG = LogFactory.getLog(goffishIndexBuilder.class);
/*     */   private PrintWriter writer;
/*  50 */   private static Semaphore binary = new Semaphore(1);
/*     */   
/*     */ 
/*  53 */   private static Semaphore cleanBinary = new Semaphore(1);
/*     */   
/*     */ 
/*  56 */   private static final Object initLock = new Object();
/*     */   private static final Object cleanLock = new Object();
/*     */ 
/*  59 */   private String basePath = ConfigFile.basePath;
/*     */   
/*     */   static enum SupportedTypes {
/*  62 */     STRING,  INT,  DOUBLE,  FLOAT,  LONG;
/*     */   }
/*     */   
/*     */ 
/*     */ 
/*     */ 
/*  68 */   private HashMap<String, SupportedTypes> vertexIndexList = new HashMap();
/*     */   
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*  74 */   private HashMap<String, SupportedTypes> edgeIndexList = new HashMap();
/*     */   
/*     */ 
/*     */   
/*     */   
/*     */ 
/*  80 */   private static boolean stored = false;
/*     */   
/*     */ 
/*  83 */   private static boolean initLucene = false;
/*     */   
/*     */ 
/*  86 */   private static boolean cleanedUp = false;
/*     */   
/*     */   private static File vertexIndexDir;
/*     */   
/*     */   private static Directory vertexDirectory;
/*     */   
/*     */   private static IndexWriter vertexIndexWriter;
/*     */   
/*     */   private static File edgeIndexDir;
/*     */   
/*     */   private static Directory edgeDirectory;
/*     */   
/*     */   private static IndexWriter edgeIndexWriter;
/*     */   
/*     */   private static Analyzer vertexAnalyzer;
/*     */   
/*     */   private static Analyzer edgeAnalyzer;
/*     */   
/*     */   private static IndexWriterConfig vertexIndexConfig;
/*     */   
/*     */   private static IndexWriterConfig edgeIndexConfig;
/*     */   
/*     */   private boolean buildListOfIndexes(Iterable<IMessage<LongWritable,Text>> messageList)
/*     */   {
				//assuming all properties provided exists
				
/*     */     
/*     */ 
/* 147 */     return true;
/*     */   }
/*     */   
/*     */ 
/*     */ 
/*     */ 
/*     */   private void initSubgraph()
/*     */     throws IOException
/*     */   {
			//No longer required to load instance of the subgraph
/*     */   }
/*     */   
/*     */ 
/*     */   private void initLuceneUtils()
/*     */     throws IOException, InterruptedException
/*     */   {
/* 181 */     if (!initLucene) {
/* 182 */       initLucene = true;
/*     */       long pseudoPid=getSubgraph().getSubgraphId().get() >> 32;

/* 184 */       vertexIndexDir = new File(this.basePath + "/index/Partition"+pseudoPid+"/vertexIndex");
/* 185 */       edgeIndexDir = new File(this.basePath + "/index/Partition"+pseudoPid+"/edgeIndex");
/*     */       
/* 187 */       vertexDirectory = FSDirectory.open(vertexIndexDir);
/* 188 */       edgeDirectory = FSDirectory.open(edgeIndexDir);
/*     */       
/* 190 */       vertexAnalyzer = new StandardAnalyzer(Version.LATEST);
/* 191 */       edgeAnalyzer = new StandardAnalyzer(Version.LATEST);
/*     */       
/* 193 */       vertexIndexConfig = new IndexWriterConfig(Version.LATEST, vertexAnalyzer);
/* 194 */       vertexIndexConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
/* 195 */       edgeIndexConfig = new IndexWriterConfig(Version.LATEST, edgeAnalyzer);
/* 196 */       edgeIndexConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
/*     */       
/* 198 */       vertexIndexWriter = new IndexWriter(vertexDirectory, vertexIndexConfig);
/* 199 */       edgeIndexWriter = new IndexWriter(edgeDirectory, edgeIndexConfig);
/*     */     }
/*     */   }
/*     */   
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */   private void buildVertexIndex()
/*     */     throws IOException
/*     */   {
				LOG.info("Writing VERTEX INDEX");
/* 213 */     for (IVertex<MapValue,MapValue,LongWritable,LongWritable> vertex : getSubgraph().getLocalVertices())
/*     */     {
/* 215 */       Document doc = new Document();
                //only Vertex Properties considered for now in arguments passed
               
/* 216 */       Iterator it = this.vertexIndexList.entrySet().iterator();
/* 217 */       while (it.hasNext())
/*     */       {
/* 219 */         Map.Entry entry = (Map.Entry)it.next();
/* 220 */         String property = (String)entry.getKey();
/* 221 */         SupportedTypes type = (SupportedTypes)entry.getValue();
/* 222 */         boolean nullValue = false;
/*     */         
		{
					 //assuming all property types are string, to be changed for other types
/* 253 */           String value = vertex.getValue().get(property).toString();
/* 254 */           if (value != null) {
/* 255 */             doc.add(new StringField(property, value.toString(), Field.Store.NO));
/*     */           } else
/* 257 */             nullValue = true;
/*     */         }
/* 259 */         if (nullValue) {
/* 260 */           doc.add(new StringField(property, "null", Field.Store.NO));
/*     */         }
/*     */       }
/* 263 */       doc.add(new LongField("id", vertex.getVertexId().get(), Field.Store.YES));
/* 264 */       doc.add(new LongField("subgraphid", getSubgraph().getSubgraphId().get(), Field.Store.YES));
/* 265 */       vertexIndexWriter.addDocument(doc);
/*     */     }
/* 267 */     vertexIndexWriter.commit();
/*     */   }
/*     */   
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */   private void buildEdgeIndex()
/*     */     throws IOException
/*     */   {   //FIXME: right now we don't use edge indices, so not making that. Later change this to incorporate edge indices
				LOG.info("Writing EDGE index");
///* 279 */     for (ITemplateEdge edge : this.subgraph.edges())
///*     */     {
///* 281 */       Document doc = new Document();
///* 282 */       Iterator it = this.edgeIndexList.entrySet().iterator();
///* 283 */       while (it.hasNext())
///*     */       {
///* 285 */         Map.Entry entry = (Map.Entry)it.next();
///* 286 */         String property = (String)entry.getKey();
///* 287 */         SupportedTypes type = (SupportedTypes)entry.getValue();
///* 288 */         boolean nullValue = false;
///*     */         
///* 290 */         if (type == SupportedTypes.DOUBLE) {
///* 291 */           Double value = (Double)this.subgraphInstance.getPropertiesForEdge(edge.getId()).getValue(property);
///* 292 */           if (value != null) {
///* 293 */             doc.add(new DoubleField(property, value.doubleValue(), Field.Store.NO));
///*     */           } else {
///* 295 */             nullValue = true;
///*     */           }
///* 297 */         } else if (type == SupportedTypes.FLOAT) {
///* 298 */           Float value = (Float)this.subgraphInstance.getPropertiesForEdge(edge.getId()).getValue(property);
///* 299 */           if (value != null) {
///* 300 */             doc.add(new FloatField(property, value.floatValue(), Field.Store.NO));
///*     */           } else {
///* 302 */             nullValue = true;
///*     */           }
///* 304 */         } else if (type == SupportedTypes.LONG) {
///* 305 */           Long value = (Long)this.subgraphInstance.getPropertiesForEdge(edge.getId()).getValue(property);
///* 306 */           if (value != null) {
///* 307 */             doc.add(new LongField(property, value.longValue(), Field.Store.NO));
///*     */           } else {
///* 309 */             nullValue = true;
///*     */           }
///* 311 */         } else if (type == SupportedTypes.INT) {
///* 312 */           Integer value = (Integer)this.subgraphInstance.getPropertiesForEdge(edge.getId()).getValue(property);
///* 313 */           if (value != null) {
///* 314 */             doc.add(new IntField(property, value.intValue(), Field.Store.NO));
///*     */           } else {
///* 316 */             nullValue = true;
///*     */           }
///*     */         } else {
///* 319 */           Object value = this.subgraphInstance.getPropertiesForEdge(edge.getId()).getValue(property);
///* 320 */           if (value != null) {
///* 321 */             doc.add(new StringField(property, value.toString(), Field.Store.NO));
///*     */           } else
///* 323 */             nullValue = true;
///*     */         }
///* 325 */         if (nullValue) {
///* 326 */           doc.add(new StringField(property, "null", Field.Store.NO));
///*     */         }
///*     */       }
///* 329 */       doc.add(new LongField("id", edge.getId(), Field.Store.YES));
///* 330 */       doc.add(new LongField("subgraphid", this.subgraph.getId(), Field.Store.YES));
///* 331 */       edgeIndexWriter.addDocument(doc);
///*     */     }
///* 333 */     edgeIndexWriter.commit();
/*     */   }
/*     */   
/*     */ 
/*     */ 
/*     */ 
/*     */   public void storeIndexedFields()
/*     */   {
              long pseudoPid=getSubgraph().getSubgraphId().get() >> 32;
/* 341 */     stored = true;
/*     */     try
/*     */     {
				//this.basePath
/* 344 */       File file = new File("/data/abhilash" + "/index/Partition"+pseudoPid+"/vertexIndexes");
/* 345 */       if (!file.exists()) {
/* 346 */         file.createNewFile();
/*     */       }
/*     */       
/* 349 */       FileOutputStream fos = new FileOutputStream(file);
/* 350 */       ObjectOutputStream oos = new ObjectOutputStream(fos);
/* 351 */       oos.writeObject(this.vertexIndexList);
/* 352 */       oos.close();
/* 353 */       fos.close();
/*     */     } catch (Exception e) {
/* 355 */       e.printStackTrace();
/*     */     }
/*     */     
/*     */     try
/*     */     {
/* 360 */       File file = new File("/data/abhilash" + "/index/Partition"+pseudoPid+"/edgeIndexes");
/* 361 */       if (!file.exists()) {
/* 362 */         file.createNewFile();
/*     */       }
/* 364 */       FileOutputStream fos = new FileOutputStream(file);
/* 365 */       ObjectOutputStream oos = new ObjectOutputStream(fos);
/* 366 */       oos.writeObject(this.edgeIndexList);
/* 367 */       oos.close();
/* 368 */       fos.close();
/*     */     } catch (Exception e) {
/* 370 */       e.printStackTrace();
/*     */     }
/*     */   }
/*     */   
/*     */ 
/*     */ 
/*     */   private void clean()
/*     */   {
/*     */     try
/*     */     {
/* 380 */       if (!cleanedUp) {
/* 381 */         cleanedUp = true;
/* 382 */         vertexIndexWriter.close();
/* 383 */         edgeIndexWriter.close();
/* 384 */         edgeAnalyzer.close();
/* 385 */         vertexAnalyzer.close();
/*     */       }
/*     */     }
/*     */     catch (IOException e) {
/* 389 */       e.printStackTrace();
/*     */     }
/*     */   }
/*     */   
/*     */   private boolean fileExists(String path)
/*     */   {
/* 395 */     return new File(path).exists();
/*     */   }
/*     */   
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */   public void compute(Iterable<IMessage<LongWritable,Text>> messageList)
/*     */   {
/* 415 */     if (getSuperstep() == 0)
/*     */     {
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/* 423 */       if (Argument==null) {
/* 424 */         LOG.info("Arguments should be of the syntax \n V:property1:property2:.. or E:property1:property2:..\n");
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */       }
/* 430 */       else if (buildListOfIndexes(messageList))
/*     */       {
/*     */         try
/*     */         {
                    String[] prop=Argument.split(":");
                     for(int i=1;i<prop.length;i++){
                       this.vertexIndexList.put(prop[i], SupportedTypes.STRING);
                     }
/* 434 */           initSubgraph();//OBSOLETE
/*     */           
/* 436 */           synchronized (initLock) {
/* 437 */             if (!initLucene) {
/* 438 */               initLuceneUtils();
/*     */             }
/*     */           }
/*     */           
/* 442 */           if (this.vertexIndexList.size() > 0) {
/* 443 */             buildVertexIndex();
/*     */           }
/* 445 */           if (this.edgeIndexList.size() > 0) {
/* 446 */             buildEdgeIndex();
/*     */           }
/*     */           
/* 449 */           synchronized (initLock) {
/* 450 */             if (!stored) {
						  LOG.info("Storing INDEX");
///* 451 */               storeIndexedFields();
/*     */             }
/*     */           }
/*     */         } catch (IOException|InterruptedException e) {
/* 455 */           e.printStackTrace();
/*     */         }
/*     */       }
/*     */       
/*     */ 
/*     */ 
/*     */ 
/* 462 */       String message = "1";
///* 463 */       sendMessage(new Long(1L).longValue(), new SubGraphMessage(message.getBytes()));
/*     */     }
/*     */     
/* 466 */     if (getSuperstep() == 1)
/*     */     {
///* 468 */       if ((this.getSubgraph().getSubgraphId().get() == 1) && 
///* 469 */         (!cleanedUp))
				synchronized(cleanLock) {
					if(!cleanedUp)
/* 470 */         		clean();
/*     */       }
/*     */     }
/*     */     
/*     */ 
/*     */ 
/*     */ 
/* 477 */     voteToHalt();
/*     */   }
/*     */   
/*     */ 
/*     */ 
/*     */   private void output(int partId, long subId, String path)
/*     */   {
/*     */     try
/*     */     {
/* 486 */       this.writer = new PrintWriter(new FileWriter("PWI_00_00.txt", true));
/*     */     }
/*     */     catch (IOException e) {
/* 489 */       e.printStackTrace();
/*     */     }
/* 491 */     this.writer.print(partId + "," + subId + "," + path + "\n");
/* 492 */     this.writer.flush();
/* 493 */     this.writer.close();
/*     */   }
/*     */
@Override
public void wrapup() {
  // TODO Auto-generated method stub
	
} 

}


/* Location:              /media/nitin/Storage_Volume/Nitin/Programming/Programs/NITIN-2015-MAJOR_PROJECT_IISc/index.jar!/edu/usc/goffish/gopher/sample/goffishIndexBuilder.class
 * Java compiler version: 7 (51.0)
 * JD-Core Version:       0.7.1
 */