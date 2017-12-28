package manning.pail;

import com.backtype.hadoop.pail.Pail;

import java.io.IOException;

/**
 * Rozdział 5.2.4
 */
public class PailVerticalPartitioning {
    public static void main(String[] args) throws IOException {
        Pail<Login> pail = Pail.create("/tmp/partitioned_logins", new PartitionedLoginPailStructure());
        Pail.TypedRecordOutputStream os = pail.openWrite();
        os.writeObject(new Login("baba jaga", 1514457889));
        os.writeObject(new Login("baba jagsza", 1482921889));
        os.close();
    }
}
