CREATE TABLE test.test_task
(
    request_id number primary key,
    request BLOB,
    response_code NUMBER,
    response BLOB,
    response_notification_timestamp DATE
);
