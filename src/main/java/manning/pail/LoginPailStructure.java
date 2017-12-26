package manning.pail;

import com.backtype.hadoop.pail.PailStructure;

import java.io.*;
import java.util.Collections;
import java.util.List;

public class LoginPailStructure implements PailStructure<Login> {

    public boolean isValidTarget(String... strings) {
        return true;
    }

    public Login deserialize(byte[] bytes) {
        DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            byte[] userBytes = new byte[dataIn.readInt()];
            int read = dataIn.read(userBytes);
            return new Login(new String(userBytes), dataIn.readLong());
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    public byte[] serialize(Login login) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);
        byte[] userBytes = login.userName.getBytes();
        try {
            dataOut.writeInt(userBytes.length);
            dataOut.write(userBytes);
            dataOut.writeLong(login.loginUnixTime);
            dataOut.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOut.toByteArray();
    }

    //poniższe metody nie są w tym przykładzie używane
    public List<String> getTarget(Login login) {
        return Collections.EMPTY_LIST;
    }

    public Class getType() {
        return Login.class;
    }
}
