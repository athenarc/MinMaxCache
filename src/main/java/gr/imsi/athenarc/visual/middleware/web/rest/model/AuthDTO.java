package gr.imsi.athenarc.visual.middleware.web.rest.model;

public final class AuthDTO {
    
    public static class LoginRequest {
        String username;
        String password;


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

        
    }

    public static class Response {
        String msg;
        String token;

        public Response(String msg, String token) {
           this.msg = msg;
           this.token = token;
        }
        
        
    }
}

