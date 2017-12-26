package manning.pail;

import com.backtype.hadoop.pail.Pail;

import java.io.IOException;

/**
 * Rozdzaił 5.2.1
 */
public class PailSimpleIO {
    public static void main(String[] args) throws IOException {
        Pail pail = Pail.create("/tmp/mypail");
        Pail.TypedRecordOutputStream os = pail.openWrite();
        os.writeObject(new byte[] {1, 2, 3});
        os.writeObject(new byte[] {1, 2, 3, 4});
        os.writeObject(new byte[] {1, 2, 3, 4, 5});
        os.close();
    }
}
