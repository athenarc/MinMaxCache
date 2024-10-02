package gr.imsi.athenarc.visual.middleware.web.rest.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.web.rest.service.InfluxDBService;
import gr.imsi.athenarc.visual.middleware.web.rest.service.PostgreSQLService;

@RestController
@RequestMapping("/data")
public class DatasetController {

    @Autowired
    private PostgreSQLService postgresService;

    @Autowired
    private InfluxDBService influxService;

    @PostMapping("/postgres/query")
    public String queryPostgres( @Valid @RequestBody Query query, @Valid @RequestBody String schema, @Valid @RequestBody String name) {
        try {
            postgresService.performQuery(query, schema, name);
            return "PostgreSQL query executed successfully.";
        } catch (Exception e) {
            return "Error executing PostgreSQL query: " + e.getMessage();
        }
    }

    @PostMapping("/influx/query")
    public String queryInflux(@Valid @RequestBody Query query) {
        try {
            influxService.performQuery(query);
            return "InfluxDB query executed successfully.";
        } catch (Exception e) {
            return "Error executing InfluxDB query: " + e.getMessage();
        }
    }
}
