package manning.jcascalog;

import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Playground;
import jcascalog.Subquery;
import jcascalog.example.Split;
import jcascalog.op.Count;
import jcascalog.op.GT;

public class JCascalogJoinSubqueries {
    public static void main(String[] args) {
//        follows();
        sentence();
    }

    private static void follows() {
        Subquery manyFollows = new Subquery("?person", "_") //uwzględnia tylko osobę śledzącą bez źródła
                .predicate(Playground.FOLLOWS)
                .predicate(new Count(), "?count")
                .predicate(new GT(), "?count", 2);

        //TODO: sprawdzić dlaczego nie działą
        Api.execute(
                new StdoutTap(),
                new Subquery("?person1", "?person2")
                        .predicate(manyFollows, "?person1") //uzywa wyniku podzapytania jako zrodla
                        .predicate(manyFollows, "?person2") //uzywa wyniku podzapytania jako zrodla
                        .predicate(Playground.FOLLOWS, "?person1", "?person2")
        );
    }

    private static void sentence() {
        Subquery wordCount = new Subquery("?word", "?count")
                .predicate(Playground.SENTENCE, "?sentence")
                .predicate(new Split(), "?sentence").out("?word")
                .predicate(new Count(), "?count");

        Api.execute(new StdoutTap(),
            new Subquery("?count", "?num-words")
                .predicate(wordCount, "_", "?count")
                .predicate(new Count(), "?num-words")
        );
    }
}
