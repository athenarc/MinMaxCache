package gr.imsi.athenarc.visual.middleware.web.rest.controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import gr.imsi.athenarc.visual.middleware.web.rest.model.AuthDTO;
import gr.imsi.athenarc.visual.middleware.web.rest.security.AuthUser;
import gr.imsi.athenarc.visual.middleware.web.rest.service.AuthService;

@RestController
@RequestMapping("/api/auth")
@Validated
public class AuthController {
    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

        @Autowired
        private AuthService authService;
        @Autowired
        private AuthenticationManager authenticationManager;

        @PostMapping("/login")
        public ResponseEntity<?> login(@RequestBody AuthDTO.LoginRequest userLogin) throws IllegalAccessException {
            Authentication authentication =
                    authenticationManager
                            .authenticate(new UsernamePasswordAuthenticationToken(
                                    userLogin.getUsername(),
                                    userLogin.getPassword()));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            AuthUser userDsetails = (AuthUser) authentication.getPrincipal();


            log.info("Token requested for user :{}", authentication.getAuthorities());
            String token = authService.generateToken(authentication);

            AuthDTO.Response response = new AuthDTO.Response("User logged in successfully", token);

            return ResponseEntity.ok(response);
        }
}