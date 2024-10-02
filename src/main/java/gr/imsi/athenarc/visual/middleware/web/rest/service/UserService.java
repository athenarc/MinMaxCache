package gr.imsi.athenarc.visual.middleware.web.rest.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.web.rest.model.User;
import gr.imsi.athenarc.visual.middleware.web.rest.repository.UserRepository;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;

    public User registerUser(String username, String password, String role) {
        User user = new User();
        user.setUsername(username);
        user.setPassword(password); // In a real application, hash the password!
        user.setRole(role);
        return userRepository.save(user);
    }

    public User findUserByUsername(String username) {
        return userRepository.findByUsername(username);
    }
}