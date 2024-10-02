package gr.imsi.athenarc.visual.middleware.web.rest.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import gr.imsi.athenarc.visual.middleware.web.rest.model.User;
import gr.imsi.athenarc.visual.middleware.web.rest.service.UserService;

@RestController
@RequestMapping("/users")
public class UserController {

    @Autowired
    private UserService userService;

    @PostMapping("/register")
    public User registerUser(@RequestParam String username, @RequestParam String password, @RequestParam String role) {
        return userService.registerUser(username, password, role);
    }

    @GetMapping("/{username}")
    public User getUser(@PathVariable String username) {
        return userService.findUserByUsername(username);
    }
}