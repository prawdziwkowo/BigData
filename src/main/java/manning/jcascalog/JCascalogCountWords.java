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

public class JCascalogCountWords {
    //patrz Playground.SENTENCE
    private static PersistentVector SENTENCE = PersistentVector.create(
            PersistentVector.create("Four score and seven years ago our fathers brought forth on this continent a new nation"),
            PersistentVector.create("conceived in Liberty and dedicated to the proposition that all men are created equal"),
            PersistentVector.create("Now we are engaged in a great civil war testing whether that nation or any nation so"),
            PersistentVector.create("conceived and so dedicated can long endure We are met on a great battlefield of that war"),
            PersistentVector.create("We have come to dedicate a portion of that field as a final resting place for those who"),
            PersistentVector.create("here gave their lives that that nation might live It is altogether fitting and proper"),
            PersistentVector.create("that we should do this"),
            PersistentVector.create("But in a larger sense we can not dedicate we can not consecrate we can not hallow"),
            PersistentVector.create("this ground The brave men living and dead who struggled here have consecrated it"),
            PersistentVector.create("far above our poor power to add or detract The world will little note nor long remember"),
            PersistentVector.create("what we say here but it can never forget what they did here It is for us the living rather"),
            PersistentVector.create("to be dedicated here to the unfinished work which they who fought here have thus far so nobly"),
            PersistentVector.create("advanced It is rather for us to be here dedicated to the great task remaining before us"),
            PersistentVector.create("that from these honored dead we take increased devotion to that cause for which they gave"),
            PersistentVector.create("the last full measure of devotion that we here highly resolve that these dead shall"),
            PersistentVector.create("not have died in vain that this nation under God shall have a new birth of freedom"),
            PersistentVector.create("and that government of the people by the people for the people shall not perish"),
            PersistentVector.create("from the earth")
    );

    public static void main(String[] args) {
        Api.execute(
            new StdoutTap(),
            new Subquery("?word", "?count")
                .predicate(SENTENCE, "?sentence")
                .predicate(new Split(), "?sentence").out("?word")
                .predicate(new Count(), "?count")
        );
    }

    public static class Split extends CascalogFunction {
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            String sentence = functionCall.getArguments().getString(0);
            for (String word: sentence.split(" ")) {
                functionCall.getOutputCollector().add(new Tuple(word));
            }
        }
    }
}
