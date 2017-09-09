package in.dream_lab.goffish.hama.succinctstructure;

import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
import edu.berkeley.cs.succinct.util.SuccinctConfiguration;
import edu.berkeley.cs.succinct.util.container.IntArrayList;

import java.io.*;
import java.util.logging.Level;

public class ConstructSuccinctFile {

  public static char[] readTextFile(File file) throws IOException {
    char[] fileData = new char[(int) file.length()];
    FileReader fr = new FileReader(file);
    fr.read(fileData);
    fr.close();
    return fileData;
  }

  public static byte[] readBinaryFile(File file) throws IOException {
    byte[] fileData = new byte[(int) file.length()];
    System.out.println("File size: " + fileData.length + " bytes");
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    dis.readFully(fileData, 0, (int) file.length());
    return fileData;
  }

  public static void construct(String inputFile,String outputFile) throws IOException {

    File file = new File(inputFile);

    FileOutputStream fos = new FileOutputStream(outputFile);
    DataOutputStream os = new DataOutputStream(fos);

    String type = "indexed-text-file";
    

    long start = System.currentTimeMillis();

//    SuccinctCore.LOG.setLevel(Level.ALL);
    switch (type) {
      case "text-file": {
        SuccinctFileBuffer.construct(readTextFile(file), os, new SuccinctConfiguration());
        break;
      }
      case "binary-file": {
        SuccinctFileBuffer.construct(readBinaryFile(file), os, new SuccinctConfiguration());
        break;
      }
      case "indexed-text-file": {
        char[] fileData = readTextFile(file);
        IntArrayList offsets = new IntArrayList();
        offsets.add(0);
        for (int i = 0; i < fileData.length; i++) {
          if (fileData[i] == '\n') {
            offsets.add(i + 1);
          }
        }
        SuccinctIndexedFileBuffer.construct(fileData, offsets.toArray(), os, new SuccinctConfiguration());
        break;
      }
      case "indexed-binary-file": {
        byte[] fileData = readBinaryFile(file);
        IntArrayList offsets = new IntArrayList();
        offsets.add(0);
        for (int i = 0; i < fileData.length; i++) {
          if (fileData[i] == '\n') {
            offsets.add(i + 1);
          }
        }
        SuccinctIndexedFileBuffer.construct(fileData, offsets.toArray(), os, new SuccinctConfiguration());
        break;
      }
      default:
        throw new UnsupportedOperationException("Unsupported mode: " + type);
    }

    long end = System.currentTimeMillis();
    System.out.println("Time to construct: " + (end - start) / 1000 + "s");

  }
}
