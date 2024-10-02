package gr.imsi.athenarc.visual.middleware.web.rest.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.PostgreSQL.JDBCConnection;

// Repository for managing datasets (AbstractDataset and its subclasses)
@Repository
public interface DatasetRepository extends JpaRepository<AbstractDataset, String> {

    AbstractDataset initializePostgresDataset(JDBCConnection connection, String schema, String id);

}