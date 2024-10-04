package gr.imsi.athenarc.visual.middleware.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.security.crypto.password.PasswordEncoder;

import gr.imsi.athenarc.visual.middleware.web.rest.config.RsaKeyConfigProperties;
import gr.imsi.athenarc.visual.middleware.web.rest.model.User;
import gr.imsi.athenarc.visual.middleware.web.rest.repository.UserRepository;


@SpringBootApplication
@EnableConfigurationProperties(RsaKeyConfigProperties.class)
@EntityScan("gr.imsi.athenarc.visual.middleware.domain")
@ComponentScan(basePackages = "gr.imsi.athenarc.visual.middleware.web.rest")
public class CacheAPI {
        private static final Logger LOG = LoggerFactory.getLogger(CacheAPI.class);

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(CacheAPI.class, args);
        ctx.registerShutdownHook();   
    }

    @Bean
    public CommandLineRunner initializeUser(UserRepository userRepository, PasswordEncoder passwordEncoder) {
        return args -> {

            User user = new User();
            user.setUsername("admin");
            user.setEmail("admin@example.com");
            user.setPassword(passwordEncoder.encode("password"));

            // Save the user to the database
            userRepository.save(user);
        };
    }
}
