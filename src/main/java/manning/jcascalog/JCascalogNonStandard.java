package manning.jcascalog;

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.operation.BufferCall;
import cascading.operation.FilterCall;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.*;
import clojure.lang.PersistentVector;
import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Subquery;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JCascalogNonStandard {
    private final static PersistentVector MY_AGES = PersistentVector.create(
            PersistentVector.create("gregory", 38),
            PersistentVector.create("ola", 35),
            PersistentVector.create("alice", 4),
            PersistentVector.create("alice", 7)
    );

    private final static PersistentVector MY_AGES2 = PersistentVector.create(
            PersistentVector.create("gregory", 38),
            PersistentVector.create("ola", 35),
            PersistentVector.create("alice", 4),
            PersistentVector.create("xxx", "28"),
            PersistentVector.create("xxx", "aaa")
    );

    public static void main(String[] args) {
//        greather();
//        increment();
//        tryParseInt();
//        sum();
//        sumBuffer();
        sumParaller();
    }

    private static void greather() {
        Api.execute(
                new StdoutTap(),
                new Subquery("?person", "?age")
                        .predicate(MY_AGES, "?person", "?age")
                        .predicate(new GreatherThanTenFilter(), "?age")
        );
    }

    private static void increment() {
        Api.execute(
                new StdoutTap(),
                new Subquery("?person", "?age", "?increment")
                        .predicate(MY_AGES, "?person", "?age")
                        .predicate(new IncrementFunction(), "?age").out("?increment")
        );
    }

    private static void tryParseInt() {
        Api.execute(
                new StdoutTap(),
                new Subquery("?person", "?age", "?int")
                        .predicate(MY_AGES2, "?person", "?age")
                        .predicate(new TryParseInteger(), "?age").out("?int")
        );
    }

    private static void sum() {
        Api.execute(
                new StdoutTap(),
                new Subquery("?person", "?sum")
                        .predicate(MY_AGES, "?person", "?age")
                        .predicate(new SumAggregator(), "?age").out("?sum")
        );
    }

    private static void sumBuffer() {
        Api.execute(
                new StdoutTap(),
                new Subquery("?person", "?sum")
                        .predicate(MY_AGES, "?person", "?age")
                        .predicate(new SumBuffer(), "?age").out("?sum")
        );
    }

    private static void sumParaller() {
        Api.execute(
                new StdoutTap(),
                new Subquery("?person", "?sum")
                        .predicate(MY_AGES, "?person", "?age")
                        .predicate(new SumParaller(), "?age").out("?sum")
        );
    }

    //filtr
    private static class GreatherThanTenFilter extends CascalogFilter {
        @Override
        public boolean isKeep(FlowProcess flowProcess, FilterCall filterCall) {
            return filterCall.getArguments().getInteger(0) > 10;
        }
    }

    //funkcja
    private static class IncrementFunction extends CascalogFunction {
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            int v = functionCall.getArguments().getInteger(0);
            functionCall.getOutputCollector().add(new Tuple(v + 1));
        }
    }

    //funkcja działajaca jako filtr
    private static class TryParseInteger extends CascalogFunction {
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            String s = functionCall.getArguments().getString(0);
            try {
                int i = Integer.parseInt(s);
                functionCall.getOutputCollector().add(new Tuple(i));
            } catch (NumberFormatException e) {

            }
        }
    }

    //agregator sumy
    private static class SumAggregator extends CascalogAggregator {
        @Override
        public void start(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
            //inicjuje wewnetrzny stan
            aggregatorCall.setContext(0);
        }

        @Override
        public void aggregate(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
            //wywolaniue dla każdej krotki, aktualizuje wewnętrzny stan
            int total = (Integer)aggregatorCall.getContext();
            int in = aggregatorCall.getArguments().getInteger(0);

            aggregatorCall.setContext(total + in);

        }

        @Override
        public void complete(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
            //emituje wyjsciową krotkę
            int total = (Integer)aggregatorCall.getContext();
            aggregatorCall.getOutputCollector().add(new Tuple(total));
        }
    }

    //agregator zwany buforem (bufory nie mogą być przetwarzane z innymi agregatorami)
    private static class SumBuffer extends CascalogBuffer {
        @Override
        public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
            Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
            int total = 0;
            while (iterator.hasNext()) {
                TupleEntry t = iterator.next();
                total += t.getInteger(0);
            }

            bufferCall.getOutputCollector().add(new Tuple(total));
        }
    }

    //najlepszy agregator; obliczenia moga byc wykonywane równolegle
    private static class SumParaller implements ParallelAgg {
        @Override
        public void prepare(FlowProcess flowProcess) {

        }

        @Override
        public List<Object> init(List<Object> input) {
            return input;//dla sumy czesciowa agregacja jest po prostu wartoscią w argumencie
        }

        @Override
        public List<Object> combine(List<Object> input1, List<Object> input2) {
            int val1 = (Integer) input1.get(0);
            int val2 = (Integer) input2.get(0);
            //aby polaczyc 2 agragacje czesciowe trzeba zsumowac 2 wartosci
            return Arrays.asList((Object)(val1 + val2));
        }
    }
}
