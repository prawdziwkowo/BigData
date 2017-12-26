package manning.pail;

import com.backtype.hadoop.pail.Pail;
import java.io.*;

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
    }

    private static void readLogins() throws IOException {
        Pail<Login> loginPail = new Pail<Login>("/tmp/paillogins");
        for (Login login: loginPail) {
            System.out.println(login.userName + " " + login.loginUnixTime);
        }
    }
}

class Login {
    public String userName;
    public long loginUnixTime;

    Login(String userName, long loginUnixTime) {
        this.userName = userName;
        this.loginUnixTime = loginUnixTime;
    }
}

