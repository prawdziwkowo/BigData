package manning.pail;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class PartitionedLoginPailStructure extends LoginPailStructure {
    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public List<String> getTarget(Login login) {
        ArrayList<String> directoryPath = new ArrayList<String>();
        Date date = new Date(login.loginUnixTime * 1000L);
        directoryPath.add(formatter.format(date));
        return directoryPath;
    }

    public boolean isValidTarget(String...strings) {
        try {
            return (formatter.parse(strings[0]) != null);
        } catch (ParseException e) {
            return false;
        }
    }
}
