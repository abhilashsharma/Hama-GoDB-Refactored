package in.dream_lab.goffish.godb;

import java.util.ArrayList;



/**
 * Path Object that contains series of Vertex,Edge,....,Vertex  
 * @author abhilash
 *
 */
public class Path {
  private ArrayList<Long> path;
  
  public Path()
  {
    path=new ArrayList<Long>();
  }
  
  
  public Path(Path p){
    path=p.path;
  }
  
  public Path(ArrayList<Long> _list){
    path=_list;
  }
  
  public Path(Long startVertex){
    path=new ArrayList<Long>();
    path.add(startVertex);
  }
  
  public void addEV(Long edge,Long Vertex){
    path.add(edge);
    path.add(Vertex);
  }
  
  
  public ArrayList<Long> getPath(){
    return path;
  }
  
  @Override
  public String toString() {
    // TODO Auto-generated method stub
    String pathStr="";
    
    for(Long vOrE: path){
      if(pathStr.length()==0){
        pathStr=vOrE.toString();
      }
      else{
        pathStr+="," + vOrE;
      }
    }
    
    return pathStr;
  }
  
  public Path getCopy(){
    ArrayList<Long> copyPath=new ArrayList<Long>();
    for(Long item:path){
      copyPath.add(Long.parseLong(item.toString()));
    }
    
    return new Path(copyPath);
  }
  
}
