package manning.thrift;

import manning.schema.Data;

import java.util.Collections;
import java.util.List;

public class DataPailStructure extends ThriftPailStructure<Data> {
    public Class getType() {
        return Data.class;
    }

    protected Data createThriftObject() {
        return new Data();
    }

    public boolean isValidTarget(String... strings) {
        return true;
    }

    public List<String> getTarget(Data data) {
        return Collections.EMPTY_LIST;
    }
}
