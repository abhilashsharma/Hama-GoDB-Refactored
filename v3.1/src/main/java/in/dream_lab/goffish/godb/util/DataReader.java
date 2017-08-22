package in.dream_lab.goffish.godb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import com.sun.xml.bind.api.RawAccessor;


public class DataReader extends DataInputStream {

	private byte[] rawBytes;
	private DataReader(ByteArrayInputStream in) {
		super(in);
	}

	public static DataReader newInstance(byte[] bytes) {		
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataReader reader = new DataReader(bais);
		reader.rawBytes = bytes;
		return reader;
	}
	
	public byte[] getBytes(){
		return rawBytes;
	}
}
