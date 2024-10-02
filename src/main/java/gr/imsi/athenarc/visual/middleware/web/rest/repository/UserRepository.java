package gr.imsi.athenarc.visual.middleware.web.rest.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import gr.imsi.athenarc.visual.middleware.web.rest.model.User;

public interface UserRepository extends JpaRepository<User, Long> {
    User findByUsername(String username);
}