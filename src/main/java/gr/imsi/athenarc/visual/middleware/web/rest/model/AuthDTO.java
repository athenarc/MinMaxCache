package gr.imsi.athenarc.visual.middleware.web.rest.model;

public final class AuthDTO {
    
    public static class LoginRequest {
        String username;
        String password;
        public LoginRequest() {
        
        }


        public LoginRequest(String username, String password) {
            this.username = username;
            this.password = password;
        }


        public Object getUsername() {
            return username;
        }


        public Object getPassword() {
            return password;
        }


        @Override
        public String toString() {
            return "LoginRequest [username=" + username + ", password=" + password + "]";
        }
        
    }

    public static class Response {
        String msg;
        String token;

        public Response() {
        
        }
        
        public Response(String msg, String token) {
           this.msg = msg;
           this.token = token;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }
        
        
    }
}

