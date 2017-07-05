ALTER TABLE METADATA
  ADD aktiviteter_sist_oppdatert TIMESTAMP DEFAULT TO_TIMESTAMP('1970-01-01 00:00:00',
                                                                'YYYY-MM-DD HH24:MI:SS') NOT NULL;
CREATE TABLE AKTIVITETER (
  AKTIVITETID   VARCHAR(20)  NOT NULL,
  AKTOERID      VARCHAR(30)  NOT NULL,
  AKTIVITETTYPE VARCHAR(255) NOT NULL,
  AVTALT        VARCHAR(20)  NOT NULL,
  FRADATO       TIMESTAMP,
  TILDATO       TIMESTAMP,
  OPPDATERTDATO TIMESTAMP    NOT NULL,
  STATUS        VARCHAR(255) NOT NULL,
  PRIMARY KEY (AKTIVITETID)
);

CREATE TABLE BRUKERSTATUS_AKTIVITETER (
  PERSONID      VARCHAR(30)  NOT NULL,
  AKTOERID      VARCHAR(30)  NOT NULL,
  AKTIVITETTYPE VARCHAR(255) NOT NULL,
  STATUS        VARCHAR(255) NOT NULL,
  PRIMARY KEY (PERSONID, AKTIVITETTYPE)
);

INSERT INTO BRUKERSTATUS_AKTIVITETER
VALUES (
  '9999',
  '9999999999991',
  'jobbsoeking',
  'fullfort'
)