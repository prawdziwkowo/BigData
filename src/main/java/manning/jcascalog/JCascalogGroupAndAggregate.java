package manning.jcascalog;

import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Playground;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.LT;

public class JCascalogGroupAndAggregate {
    public static void main(String[] args) {
        age();
    }

    private static void age() {
        Api.execute(
            new StdoutTap(),
            new Subquery("?gender", "?count")
                .predicate(Playground.AGE, "?person", "?age")
                .predicate(Playground.GENDER, "?person", "?gender")
                .predicate(new LT(), "?age", 30)
                .predicate(new Count(), "?count")
        );
    }
}
