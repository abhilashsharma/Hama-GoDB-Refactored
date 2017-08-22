package in.dream_lab.goffish.godb;

import java.util.ArrayList;



/**
 * Path Object that contains series of Vertex,Edge,....,Vertex  
 * @author abhilash
 *
 */
public class PathWithDir {
  public long startVertex;
  public ArrayList<EVPair> path;
  
  public PathWithDir()
  {
    path=new ArrayList<EVPair>();
  }
  
  
  public PathWithDir(PathWithDir p){
    this.startVertex=p.startVertex;
    path=p.path;
  }
  
  public PathWithDir(long _startVertex,ArrayList<EVPair> _list){
    startVertex=_startVertex;
    path=_list;
  }
  
  public PathWithDir(Long startVertex){
    path=new ArrayList<EVPair>();
    this.startVertex=startVertex;
  }
  
  public void addEV(Long edge,Long vertex,boolean dir){
    EVPair ev=new EVPair(edge,vertex , dir);
    path.add(ev);
  }
  public boolean equals(Object obj){
    PathWithDir other=(PathWithDir) obj;
    boolean flag=true;
    if(this.path.size()!=other.path.size()){
      flag=false;
    }
    else if(this.startVertex!=other.startVertex){
      flag=false;
    }
    else{
      for(int i=0;i<this.path.size();i++){
        if(!this.path.get(i).equals(other.path.get(i))){
          flag=false;
          break;
        }
      }
    }
    
    return flag;
  }
  
  public PathWithDir getPath(){
    return this;
  }
  
  @Override
  public String toString() {
    // TODO Auto-generated method stub
    String pathStr="";
    pathStr+=startVertex;
    for(EVPair vOrE: path){
      
        pathStr+="," + vOrE.toString();
      
    }
   
    
    return pathStr;
  }
  
  public PathWithDir getCopy(){
    ArrayList<EVPair> copyPath=new ArrayList<EVPair>();
    for(EVPair item:path){
      copyPath.add(new EVPair(item.edgeId,item.vertexId,item.direction));
    }
    
    return new PathWithDir(this.startVertex,copyPath);
  }
  
  
  
  public class EVPair{
    public long edgeId;
    public long vertexId;
    public boolean direction;//true for out and false for in
    public EVPair(long _edgeId,long _vertexId,boolean _dir){
      this.edgeId=_edgeId;
      this.vertexId=_vertexId;
      this.direction=_dir;
    }
    
    public String toString(){
      return edgeId + "," + vertexId;
    }
    public boolean equals(Object obj){
      EVPair other=(EVPair) obj;
      return this.direction==other.direction && this.edgeId==other.edgeId && this.vertexId==other.vertexId;
    }
  }



  public void insert(PathWithDir path2) {
    // TODO Auto-generated method stub
    PathWithDir newPath=new PathWithDir(path2.startVertex);
    for(EVPair ev: path2.path){
      newPath.addEV(ev.edgeId, ev.vertexId, ev.direction);
    }
    for(EVPair ev: this.path){
      newPath.addEV(ev.edgeId, ev.vertexId, ev.direction);
    }
    
    this.startVertex=newPath.startVertex;
    this.path=newPath.path;
  }


  public void append(PathWithDir path2) {
    // TODO Auto-generated method stub
//    System.out.println("Appending Results:Stored results:" + path2.toString() +"  partial results:"+ this.toString() );
    PathWithDir newPath=new PathWithDir(path2.startVertex);
    
    for(EVPair ev: path2.path){
      newPath.addEV(ev.edgeId, ev.vertexId, ev.direction);
    }
    
    for(EVPair ev: this.path){
      newPath.addEV(ev.edgeId, ev.vertexId, ev.direction);
    }
    
    this.startVertex=newPath.startVertex;
    this.path=newPath.path;
  }
  
  
}
