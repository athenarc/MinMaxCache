package gr.imsi.athenarc.visual.middleware.web.rest.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.methods.VisualQueryResults;
import gr.imsi.athenarc.visual.middleware.web.rest.model.QueryDTO;
import gr.imsi.athenarc.visual.middleware.web.rest.service.InfluxDBService;
import gr.imsi.athenarc.visual.middleware.web.rest.service.PostgreSQLService;

@RestController
@RequestMapping("/api/data")
public class DatasetController {

    @Autowired
    private PostgreSQLService postgresService;

    @Autowired
    private InfluxDBService influxService;

    /* POSTGRES */
    @GetMapping("/postgres/dataset/{schema}/{id}")
    public ResponseEntity<AbstractDataset> getDatasetPostgres(@PathVariable("schema") String schema, @PathVariable("id") String id) {
        try {
            PostgreSQLDataset dataset = postgresService.getDatasetById(schema, id);
            return ResponseEntity.ok(dataset);
        } catch (RuntimeException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

   @PostMapping("/postgres/query")
    public ResponseEntity<QueryDTO.QueryResponse> queryPostgres(@Valid @RequestBody QueryDTO.QueryRequest queryRequest) {
        try {
            VisualQueryResults queryResults = postgresService.performQuery(queryRequest.query);

            // Wait for the query result (or handle asynchronously for better UX)
            QueryDTO.QueryResponse queryResponse = new QueryDTO.QueryResponse(
                "PostgresDB query executed successfully.", queryResults);
            return ResponseEntity.ok(queryResponse);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
    

    /* INFLUX  */
    @GetMapping("/influx/dataset/{schema}/{id}")
    public ResponseEntity<AbstractDataset> getDatasetInflux(@PathVariable("schema") String schema, @PathVariable("id") String id) {
        try {
            InfluxDBDataset dataset = influxService.getDatasetById(schema, id);
            return ResponseEntity.ok(dataset);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @PostMapping("/influx/query")
    public ResponseEntity<QueryDTO.QueryResponse> queryInflux(@Valid @RequestBody QueryDTO.QueryRequest queryRequest) {
        try {
            VisualQueryResults queryResults = influxService.performQuery(queryRequest.query);
       
            QueryDTO.QueryResponse queryResponse = new QueryDTO.QueryResponse(
                "InfluxDB query executed successfully.", queryResults);
            return ResponseEntity.ok(queryResponse);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
}
