package gr.imsi.athenarc.visual.middleware.web.rest.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import gr.imsi.athenarc.visual.middleware.domain.dataset.InfluxDBDataset;

public interface InfluxDBDatasetRepository extends JpaRepository<InfluxDBDataset, String> {
}