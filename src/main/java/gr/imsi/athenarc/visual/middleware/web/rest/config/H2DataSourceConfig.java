package gr.imsi.athenarc.visual.middleware.web.rest.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import jakarta.persistence.EntityManagerFactory;

import javax.sql.DataSource;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
    basePackages = {"your.package.user.repository"}, // Repository package for user management
    entityManagerFactoryRef = "userEntityManagerFactory",
    transactionManagerRef = "userTransactionManager"
)
public class H2DataSourceConfig {

    @Bean(name = "h2DataSource")
    @Primary
    public DataSource h2DataSource() {
        return DataSourceBuilder.create()
                .url("jdbc:h2:mem:testdb")
                .driverClassName("org.h2.Driver")
                .username("sa")
                .password("password")
                .build();
    }

    @Bean(name = "userEntityManagerFactory")
    @Primary
    public LocalContainerEntityManagerFactoryBean userEntityManagerFactory(
            EntityManagerFactoryBuilder builder, @Qualifier("h2DataSource") DataSource dataSource) {
        return builder
                .dataSource(dataSource)
                .packages("your.package.user.entity")  // Entity package for user management
                .persistenceUnit("user")
                .build();
    }

    @Bean(name = "userTransactionManager")
    @Primary
    public PlatformTransactionManager userTransactionManager(
            @Qualifier("userEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
