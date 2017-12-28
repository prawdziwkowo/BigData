package manning.thrift;

import com.backtype.hadoop.pail.Pail;
import java.io.IOException;

public class Split {

    //todo: zobaczyc jak to działa
    public static void main(String[] args) throws IOException {
        Pail pail = Pail.create("/tmp/partitioned_stuct", new SplitDataPailStructure());
    }
}
