/*
DROP TABLE tb_audit;
DROP TABLE tb_table_checksum;
*/
CREATE TABLE tb_audit (
    id_audit    INT          NOT NULL IDENTITY(1, 1),
    primary_key BIGINT       NOT NULL,
    table_name  VARCHAR(255) NOT NULL,
    change_type CHAR(6)      NOT NULL,
    change_date DATETIME     NOT NULL DEFAULT (GETDATE()),

    CONSTRAINT pk_audit PRIMARY KEY (id_audit),
    CONSTRAINT ck_audit_type CHECK (change_type IN ('INSERT', 'UPDATE', 'DELETE'))
);

CREATE TABLE tb_table_checksum (
    id_table_checksum INT          NOT NULL IDENTITY(1, 1),
    table_name        VARCHAR(255) NOT NULL,
    primary_key       BIGINT       NOT NULL,
    crc32             BIGINT       NOT NULL,

    CONSTRAINT pk_table_checksum PRIMARY KEY (id_table_checksum)
);