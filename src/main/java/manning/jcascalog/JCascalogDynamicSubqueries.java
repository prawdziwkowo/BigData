package manning.jcascalog;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import clojure.lang.PersistentVector;
import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.op.Count;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

public class JCascalogDynamicSubqueries {
    public static void main(String[] args) {
        Api.execute(new StdoutTap(),
                new Subquery("?buyer", "?count")
                    .predicate(buyerNumTransactions("hdfs://localhost/java/buyers.txt"), "?buyer", "_", "_", "_")
                    .predicate(new Count(), "?count")
        );
    }

    private static Subquery buyerNumTransactions(String path) {
        return new Subquery("?buyer", "?seller", "?amt", "?timestamp")
                .predicate(Api.hfsTextline(path), "?line")
                .predicate(new ParseRecord(), "?line").out("?buyer", "?seller", "?amt", "?timestamp");
    }

    private static class ParseRecord extends CascalogFunction {
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            String line = functionCall.getArguments().getString(0);
            JSONParser parser = new JSONParser();
            try {
                Map parsed = (Map) parser.parse(line);
                functionCall.getOutputCollector().add(new Tuple(parsed.get("buyer"),
                                                                parsed.get("seller"),
                                                                parsed.get("amt"),
                                                                parsed.get("timestamp")));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
