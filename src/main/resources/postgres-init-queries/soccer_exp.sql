CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.soccer_exp_tmp(
    timestamp        TIMESTAMP NOT NULL
    ,x      FLOAT
    ,y      FLOAT
    ,z     FLOAT
    ,abs_vel    FLOAT
    ,abs_accel FLOAT
    ,vx      FLOAT
    ,vy      FLOAT
    ,vz     FLOAT
    ,ax      FLOAT
    ,ay      FLOAT
    ,az     FLOAT

);

COPY more.soccer_exp_tmp(timestamp, x, y, z, abs_vel, abs_accel, vx, vy, vz, ax, ay, az)
    FROM '%path'
    DELIMITER ','
    CSV HEADER;

CREATE TABLE more.soccer_exp(
    timestamp   TIMESTAMP NOT NULL
    ,id VARCHAR NOT NULL
    ,value      FLOAT
    ,PRIMARY KEY(id, timestamp)
);

--date_part('epoch', timestamp) * 1000,

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'abs_accel', abs_accel FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'abs_vel', abs_vel FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'ax', ax FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'ay', ay FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'az', az FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'vx', vx FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'vy', vy FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'vz', vz FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'x', x FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'y', y FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(timestamp, id, value)
SELECT timestamp, 'z', z FROM more.soccer_exp_tmp;



--
--

DROP TABLE more.soccer_exp_tmp;

analyze more.soccer_exp;
create statistics s_ext_depend_soccer_exp(dependencies) on timestamp,id from more.soccer_exp ;
