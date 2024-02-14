CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.intel_lab_exp_tmp(
   timestamp        TIMESTAMP NOT NULL
  ,moteid      FLOAT
  ,temperature      FLOAT
  ,humidity     FLOAT
  ,light    FLOAT
  ,voltage FLOAT
);

COPY more.intel_lab_exp_tmp(timestamp, moteid, temperature,
humidity, light, voltage)
FROM '%path'
DELIMITER ','
CSV HEADER;

CREATE TABLE more.intel_lab_exp(
  timestamp   TIMESTAMP NOT NULL
  ,id         VARCHAR NOT NULL
  ,value      FLOAT
  ,PRIMARY KEY(id, timestamp)
);

INSERT INTO more.intel_lab_exp(timestamp, id, value)
SELECT timestamp, 'humidity', humidity FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(timestamp, id, value)
SELECT timestamp, 'light', light FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(timestamp, id, value)
SELECT timestamp, 'moteid', moteid FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(timestamp, id, value)
SELECT timestamp, 'temperature', temperature FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(timestamp, id, value)
SELECT timestamp, 'voltage', voltage FROM more.intel_lab_exp_tmp;

DROP TABLE more.intel_lab_exp_tmp;


analyze more.intel_lab_exp;
create statistics s_ext_depend_intel_lab_exp(dependencies) on timestamp,id from more.intel_lab_exp ;
