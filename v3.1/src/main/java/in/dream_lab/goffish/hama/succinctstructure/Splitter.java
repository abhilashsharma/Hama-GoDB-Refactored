package in.dream_lab.goffish.hama.succinctstructure;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
/**
 * Created by sandy on 11/29/17.
 */
/**
 * Created by sandy on 11/25/17.
 */
public class Splitter {
    private static long[] pow;
    private Splitter()
    {
        pow = new long[20];
        pow[0] = 1;
        for (int i = 1;i < 20; i++)
            pow[i] = pow[i-1] * 10;
    }
    public static Splitter createSplitter()
    {
        return new Splitter();
    }
    private final boolean isDigit(byte b)
    {
        return b >=48 && b<=57;
    }
    private final boolean isUpperCharacter(byte b)
    {
        return b >= 65 && b<=90;
    }
    private final boolean isLowerCharacter(byte b)
    {
        return b >= 97 && b<= 122;
    }
    public final  LongArrayList splitLong(byte[] bytes)
    {
        LongArrayList values = new LongArrayList();
        long x;
        int len, index;
        len = bytes.length;
        index = len - 1;
        while(index >=0){
            x = 0;
            while (index >=0  && isDigit(bytes[index])) {
                x += (bytes[index] - '0') * pow[len - index - 1];
                index--;
            }
            if (index != len - 1)
                values.add(0, x);
            len = index;
            index = len - 1;
        }
        return values;
    }
    public final ObjectArrayList splitString(byte[] bytes)
    {
        ObjectArrayList values = new ObjectArrayList();
        StringBuilder x;
        int index = 0;
        while (index < bytes.length)
        {
            x = new StringBuilder();
            while (index < bytes.length && (isLowerCharacter(bytes[index]) || isUpperCharacter(bytes[index]) || isDigit(bytes[index]) ))
            {
              
                
                    x.append((char)bytes[index]);
                index++;
            }
            values.add(x.toString());
            index++;
        }
        return values;
    }
    public static void main(String args[]) {
        Splitter split = Splitter.createSplitter();
        LongArrayList values = split.splitLong(args[0].getBytes());
        for (long val : values)
            System.out.print(val + ", ");

    }

}
