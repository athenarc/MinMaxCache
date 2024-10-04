package gr.imsi.athenarc.visual.middleware.web.rest.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

    @PostMapping("/postgres/query")
    public ResponseEntity<QueryDTO.QueryResponse> queryPostgres( @Valid @RequestBody QueryDTO.QueryRequest queryRequest) {
        try {
            QueryDTO.QueryResponse queryResponse = new QueryDTO.QueryResponse("PostgresDB query executed successfully.", 
                postgresService.performQuery(queryRequest.query, queryRequest.schema, queryRequest.table));
                return ResponseEntity.ok().body(queryResponse); 
            } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/influx/query")
    public ResponseEntity<QueryDTO.QueryResponse> queryInflux(@Valid @RequestBody QueryDTO.QueryRequest queryRequest) {
        try {
            QueryDTO.QueryResponse queryResponse = new QueryDTO.QueryResponse("InfluxDB query executed successfully.",
                influxService.performQuery(queryRequest.query, queryRequest.schema, queryRequest.table));
            return ResponseEntity.ok().body(queryResponse); 
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
