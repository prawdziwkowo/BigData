package manning.pail;

import com.backtype.hadoop.pail.Pail;

import java.io.IOException;

/**
 * Rozdział 5.2.3
 */
public class PailConsolidation {
    public static void main(String[] args) throws IOException {
        Pail<Login> loginPail = new Pail<Login>("/tmp/paillogins");
        Pail<Login> updatePail = new Pail<Login>("/tmp/pailloginsu");

        loginPail.absorb(updatePail);
        loginPail.consolidate();

        readLogins();
    }

    private static void readLogins() throws IOException {
        Pail<Login> loginPail = new Pail<Login>("/tmp/paillogins");
        for (Login login: loginPail) {
            System.out.println(login.userName + " " + login.loginUnixTime);
        }
    }
}
