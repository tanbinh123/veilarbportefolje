CREATE SEQUENCE feilet_kafka_melding_serial AS BIGINT START 1;

ALTER TABLE FEILET_KAFKA_MELDING
    ALTER COLUMN ID SET DEFAULT nextval('feilet_kafka_melding_serial');

CREATE SEQUENCE KAFKA_CONSUMER_RECORD_ID_SEQ;