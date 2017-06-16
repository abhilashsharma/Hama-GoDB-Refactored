package in.dream_lab.goffish.godb;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;


public class MapValue implements Writable
{
  public Map<String, String> Value;

  public MapValue(){
    Value=new HashMap<String,String>();
  }
  
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (Value == null) {
      Value = new HashMap<>();
    }
    dataOutput.writeInt(Value.size());
    for (Map.Entry<String, String> entry : Value.entrySet()) {
      dataOutput.writeChars(entry.getKey());
      dataOutput.writeChars(entry.getValue());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = dataInput.readInt();
    Value = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      Value.put(dataInput.readLine(), dataInput.readLine());
    }
  }
  
  public String get(String key){
    return Value.get(key);
  }
  
  public void put(String key,String value){
    Value.put(key,value);
  }
  
  
}
