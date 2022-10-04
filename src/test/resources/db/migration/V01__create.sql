CREATE SCHEMA test;

CREATE TABLE test.test_task
(
    request_id bigint primary key,
    request bytea,
    response_code integer,
    response bytea,
    response_notification_timestamp timestamp
);
