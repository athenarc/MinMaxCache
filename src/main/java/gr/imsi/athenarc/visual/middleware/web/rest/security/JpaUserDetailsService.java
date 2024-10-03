package gr.imsi.athenarc.visual.middleware.web.rest.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.web.rest.repository.UserRepository;

@Service
public class JpaUserDetailsService implements UserDetailsService {

    @Autowired
    private UserRepository userRepository;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        AuthUser user = userRepository
                .findByUsername(username)
                .map(AuthUser::new)
                .orElseThrow(() -> new UsernameNotFoundException("User name not found: " + username));

        return user;

    }
}
