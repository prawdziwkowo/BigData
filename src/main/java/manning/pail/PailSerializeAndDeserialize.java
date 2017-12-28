package manning.pail;

import com.backtype.hadoop.pail.Pail;
import java.io.*;

/**
 * Rozdział 5.2.2
 */
public class PailSerializeAndDeserialize {
    public static void main(String[] args) throws IOException {
        writeLogins();
        readLogins();
    }

    private static void writeLogins() throws IOException {
        Pail loginPail = Pail.create("/tmp/paillogins", new LoginPailStructure());
        Pail.TypedRecordOutputStream out = loginPail.openWrite();
        out.writeObject(new Login("Grzegorz", 1234567));
        out.writeObject(new Login("Grzybek", 23456789));
        out.close();

        Pail updatePail = Pail.create("/tmp/pailloginsu", new LoginPailStructure());
        out = updatePail.openWrite();
        out.writeObject(new Login("Grzegorz1", 12345678));
        out.writeObject(new Login("Grzybek1", 234567890));
        out.close();
    }

    private static void readLogins() throws IOException {
        Pail<Login> loginPail = new Pail<Login>("/tmp/paillogins");
        for (Login login: loginPail) {
            System.out.println(login.userName + " " + login.loginUnixTime);
        }
    }
}


