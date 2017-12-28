package manning.pail;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.SequenceFileFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PailFileFormatts {
    public static void main(String[] args) throws IOException {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
        options.put(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK);

        Pail<Login> compressed = Pail.create("/tmp/pailcompressed",
                new PailSpec("SequenceFile", options, new LoginPailStructure()));

        Pail.TypedRecordOutputStream os = compressed.openWrite();
        os.writeObject(new Login("baba jaga", 1514457889));
        os.writeObject(new Login("baba jagsza", 1482921889));
        os.close();

    }
}
