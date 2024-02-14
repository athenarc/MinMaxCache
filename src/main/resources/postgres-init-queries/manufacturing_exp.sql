CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.manufacturing_exp_tmp(
    timestamp TIMESTAMP NOT NULL
    ,value_1  FLOAT
    ,value_2  FLOAT
    ,value_3  FLOAT
    ,value_4 FLOAT
    ,value_5 FLOAT
    ,value_6 FLOAT
    ,value_7 FLOAT
);

COPY more.manufacturing_exp_tmp(timestamp,value_1,value_2,value_3,value_4,value_5,value_6,value_7)
    FROM '%path'
    DELIMITER ','
    CSV HEADER;

CREATE TABLE more.manufacturing_exp(
    timestamp   TIMESTAMP NOT NULL
    ,id VARCHAR NOT NULL
    ,value      FLOAT
    ,PRIMARY KEY(id, timestamp)
);

INSERT INTO more.manufacturing_exp(timestamp, id, value)
SELECT  timestamp, 'value_1', value_1, FROM more.manufacturing_exp_tmp;

INSERT INTO more.manufacturing_exp(timestamp, id, value)
SELECT  timestamp, 'value_2', value_2 FROM more.manufacturing_exp_tmp;

INSERT INTO more.manufacturing_exp(timestamp, id, value)
SELECT  timestamp, 'value_3', value_3 FROM more.manufacturing_exp_tmp;

INSERT INTO more.manufacturing_exp(timestamp, id, value)
SELECT  timestamp, 'value_4', value_4 FROM more.manufacturing_exp_tmp;

INSERT INTO more.manufacturing_exp(timestamp, id, value)
SELECT  timestamp, 'value_5', value_5 FROM more.manufacturing_exp_tmp;

INSERT INTO more.manufacturing_exp(timestamp, id, value)
SELECT  timestamp, 'value_6', value_6 FROM more.manufacturing_exp_tmp;

INSERT INTO more.manufacturing_exp(timestamp, id, value)
SELECT  timestamp, 'value_7', value_7 FROM more.manufacturing_exp_tmp;


DROP TABLE more.manufacturing_exp_tmp;

analyze more.manufacturing_exp;
create statistics s_ext_depend_manufacturing_exp(dependencies) on timestamp,id from more.manufacturing_exp ;
