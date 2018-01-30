package manning.jcascalog;

import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Playground;
import jcascalog.PredicateMacroTemplate;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.Div;
import jcascalog.op.Sum;

public class JCascalogMacros {
    public static void main(String[] args) {
        average();
    }

    private static void average() {
        PredicateMacroTemplate average =
            PredicateMacroTemplate.build("?val")
                .out("?avg")
                .predicate(new Count(), "?count")
                .predicate(new Sum(), "?val").out("?sum")
                .predicate(new Div(), "?sum", "?count").out("?avg");

        Api.execute(
                new StdoutTap(),
                new Subquery("?ret")
                        .predicate(Playground.INTEGER, "?n")
                        .predicate(average, "?n").out("?ret")
        );
    }
}
