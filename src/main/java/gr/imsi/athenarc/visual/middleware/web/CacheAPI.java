package gr.imsi.athenarc.visual.middleware.web;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import gr.imsi.athenarc.visual.middleware.web.rest.config.RsaKeyConfigProperties;
import gr.imsi.athenarc.visual.middleware.web.rest.model.User;
import gr.imsi.athenarc.visual.middleware.web.rest.repository.UserRepository;


@SpringBootApplication
@EnableConfigurationProperties(RsaKeyConfigProperties.class)
@ComponentScan(basePackages = "gr.imsi.athenarc.visual.middleware.web.rest")
public class CacheAPI {
    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(CacheAPI.class, args);
        ctx.registerShutdownHook();   
    }

    @Bean
    public CommandLineRunner initializeUser(UserRepository userRepository, BCryptPasswordEncoder passwordEncoder) {
        return args -> {

                User user = new User();
                user.setUsername("exampleuser");
                user.setEmail("example@gmail.com");
                user.setPassword(passwordEncoder.encode("examplepassword"));

                // Save the user to the database
                userRepository.save(user);
        };
    }
}
