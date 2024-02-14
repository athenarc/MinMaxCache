CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.synthetic512m_tmp(
     timestamp        TIMESTAMP NOT NULL PRIMARY KEY
    ,value_1      FLOAT NOT NULL
    ,value_2      FLOAT NOT NULL
    ,value_3     FLOAT NOT NULL
    ,value_4    FLOAT NOT NULL
    ,value_5    FLOAT NOT NULL

);
COPY more.synthetic512m_tmp(timestamp, value_1, value_2, value_3, value_4, value_5)
    FROM '%path'
    DELIMITER ','
    CSV HEADER;


CREATE TABLE more.synthetic512m(
    timestamp  TIMESTAMP NOT NULL
    ,id         VARCHAR NOT NULL
    ,value      FLOAT
    ,PRIMARY KEY(id, timestamp)
);

INSERT INTO more.synthetic512m(epoch, timestamp, value, id, col)
SELECT  timestamp, 'value_1', value_1 FROM more.synthetic512m_tmp;

INSERT INTO more.synthetic512m(epoch, timestamp, value, id, col)
SELECT  timestamp, 'value_2', value_2 FROM more.synthetic512m_tmp;

INSERT INTO more.synthetic512m(epoch, timestamp, value, id, col)
SELECT  timestamp, 'value_3', value_3 FROM more.synthetic512m_tmp;

INSERT INTO more.synthetic512m(epoch, timestamp, value, id, col)
SELECT  timestamp, 'value_4', value_4 FROM more.synthetic512m_tmp;

INSERT INTO more.synthetic512m(epoch, timestamp, value, id, col)
SELECT  timestamp, 'value_5', value_5 FROM more.synthetic512m_tmp;

DROP TABLE more.synthetic512m_tmp;

analyze more.manufacturing_exp;
create statistics s_ext_depend_synthetic512m(dependencies) on timestamp,id from more.synthetic512m ;


