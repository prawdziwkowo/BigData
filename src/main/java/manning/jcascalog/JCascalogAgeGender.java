package manning.jcascalog;

import clojure.lang.PersistentVector;
import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Playground;
import jcascalog.Subquery;


public class JCascalogAgeGender {

    private final static PersistentVector MY_AGES = PersistentVector.create(
            PersistentVector.create("gregory", 38),
            PersistentVector.create("ola", 35),
            PersistentVector.create("alice", 28)
    );

    public static void main(String[] args) {
//        join();
//        leftJoin();
//        lefRightJoin();
//        union();
//        combine();
    }

    //odpowiednik inner join
    private static void join() {
        Api.execute(
            new StdoutTap(),
            new Subquery("?person", "?age", "?gender")
                .predicate(Playground.AGE, "?person", "?age")
                .predicate(Playground.GENDER, "?person", "?gender")
        );
    }

    private static void leftJoin() {
        Api.execute(
            new StdoutTap(),
            new Subquery("?person", "?age", "!!gender")
                .predicate(Playground.AGE, "?person", "?age")
                .predicate(Playground.GENDER, "?person", "!!gender")
        );
    }

    private static void lefRightJoin() {
        Api.execute(
            new StdoutTap(),
            new Subquery("?person", "!!age", "!!gender")
                .predicate(Playground.AGE, "?person", "!!age")
                .predicate(Playground.GENDER, "?person", "!!gender")
        );
    }

    //laczy dwa zbiory i usuwa powtorzenia
    private static void union() {

        Api.execute(
            new StdoutTap(),
            new Subquery("?person", "?age")
                .predicate(Api.union(Playground.AGE, MY_AGES), "?person", "?age")

        );
    }

    //laczy dwa zbiory
    private static void combine() {
        Api.execute(
            new StdoutTap(),
            new Subquery("?person", "?age")
                .predicate(Api.combine(Playground.AGE, MY_AGES), "?person", "?age")

        );
    }
}
