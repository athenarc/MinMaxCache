package gr.imsi.athenarc.visual.middleware.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
public class CacheAPI {
    public static void main(String[] args) {
    ConfigurableApplicationContext ctx = SpringApplication.run(CacheAPI.class, args);
    ctx.registerShutdownHook();    }
}
