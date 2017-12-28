package manning.pail;

class Login {
    public String userName;
    public long loginUnixTime;

    Login(String userName, long loginUnixTime) {
        this.userName = userName;
        this.loginUnixTime = loginUnixTime;
    }
}
