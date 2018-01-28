package manning.jcascalog;

import cascading.operation.aggregator.Average;
import clojure.lang.PersistentVector;
import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.op.*;

public class JCascalogSummary {
    //?a ?b
    private final static PersistentVector VAL1 = PersistentVector.create(
            PersistentVector.create("a", 1),
            PersistentVector.create("b", 2),
            PersistentVector.create("c", 5),
            PersistentVector.create("d", 12),
            PersistentVector.create("d", 1)
    );

    //?a ?c
    private final static PersistentVector VAL2 = PersistentVector.create(
            PersistentVector.create("b", 4),
            PersistentVector.create("b", 6),
            PersistentVector.create("c", 3),
            PersistentVector.create("d", 15)
    );

    public static void main(String[] args) {
        Api.execute(
            new StdoutTap(),
            new Subquery("?a", "?avg")
                    //generatory dla danych wejsciowy
                .predicate(VAL1, "?a", "?b")
                .predicate(VAL2, "?a", "?c")
                    //funkcja i filtr przed agregatorami
                .predicate(new Multiply(), 2, "?b").out("?double-b")
                .predicate(new LT(), "?b", "?c")
                    //kilka agregatorów
                .predicate(new Count(), "?count")
                .predicate(new Sum(), "?double-b").out("?sum")
                .predicate(new Div(), "?sum", "?count").out("?avg")
//                .predicate(new Average(), "?avg") zobaczyc jak ten predykat używać
                    //predykaty stosowane po agegatorach
                .predicate(new Div(), "?sum", "?count").out("?double-avg")
                .predicate(new LT(), "?double-avg", 50)

        );
    }
}
