SET RETENTION_TIME 0;

-- Sequences
CREATE SEQUENCE IF NOT EXISTS seq_validation_run START WITH 1 INCREMENT BY 1 MINVALUE 1 CYCLE;
CREATE SEQUENCE IF NOT EXISTS seq_validation_check START WITH 1 INCREMENT BY 1 MINVALUE 1 CYCLE;


-- Table TAL
CREATE TABLE IF NOT EXISTS tal (
  tal_id INTEGER AUTO_INCREMENT,
  tal_public_key VARCHAR NULL,
  tal_name VARCHAR NULL,
  tal_loaded_cer BINARY NULL,
  PRIMARY KEY (tal_id));


-- Table TAL_URI
CREATE TABLE IF NOT EXISTS tal_uri (
  tau_id INTEGER AUTO_INCREMENT,
  tal_id INTEGER,
  tau_location VARCHAR(400) NOT NULL,
  PRIMARY KEY (tau_id),
  FOREIGN KEY (tal_id) REFERENCES TAL (tal_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS tal_uri_tal_id_idx ON TAL_URI (tal_id ASC);


-- Table RPKI_REPOSITORY
CREATE TABLE IF NOT EXISTS rpki_repository (
    rpr_id BIGINT AUTO_INCREMENT,
    rpr_updated_at VARCHAR NOT NULL,
    rpr_location_uri VARCHAR(400),
    rpr_parent_repository_id BIGINT,
    PRIMARY KEY (rpr_id),
    UNIQUE (rpr_location_uri),
    FOREIGN KEY (rpr_parent_repository_id) REFERENCES rpki_repository (rpr_id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS rpki_repository__parent_repository_id_idx ON rpki_repository (rpr_parent_repository_id ASC);


-- Table RPKI_REPOSITORY_TRUST_ANCHORS
CREATE TABLE IF NOT EXISTS rpki_repository_trust_anchors (
    rpr_id BIGINT,
    tal_id INTEGER,
    PRIMARY KEY (rpr_id, tal_id),
    FOREIGN KEY (tal_id) REFERENCES tal (tal_id) ON DELETE CASCADE,
    FOREIGN KEY (rpr_id) REFERENCES rpki_repository (rpr_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS rpki_repository_trust_anchors__trust_anchor_id_idx ON rpki_repository_trust_anchors (tal_id ASC);


-- Table RPKI_OBJECT
CREATE TABLE IF NOT EXISTS rpki_object (
    rpo_id BIGINT AUTO_INCREMENT,
    rpo_type VARCHAR NOT NULL,
    rpo_serial_number BINARY,
    rpo_signing_time VARCHAR,
    rpo_last_marked_reachable_at VARCHAR NOT NULL,
    rpo_authority_key_identifier BINARY,
    rpo_subject_key_identifier BINARY,
    rpo_sha256 BINARY NOT NULL,
    rpo_is_ca BIT NOT NULL,
    PRIMARY KEY (rpo_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS rpki_object__sha256_idx ON rpki_object (rpo_sha256 ASC);
CREATE INDEX IF NOT EXISTS rpki_object__authority_key_identifier_idx ON rpki_object (rpo_authority_key_identifier ASC);
CREATE INDEX IF NOT EXISTS rpki_object__subject_key_identifier_idx ON rpki_object (rpo_subject_key_identifier ASC);


-- Table RPKI_OBJECT_LOCATIONS
CREATE TABLE IF NOT EXISTS rpki_object_locations (
    rpo_id BIGINT,
    rpo_locations VARCHAR(400) NOT NULL,
    PRIMARY KEY (rpo_id, rpo_locations),
    FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE
);


-- Table RPKI_REPOSITORY_RPKI_OBJECT
CREATE TABLE IF NOT EXISTS rpki_repository_rpki_object (
    rpr_id BIGINT,
    rpo_id BIGINT,
    PRIMARY KEY (rpr_id, rpo_id),
    FOREIGN KEY (rpr_id) REFERENCES rpki_repository (rpr_id) ON DELETE CASCADE,
    FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE
);


-- Table ENCODED_RPKI_OBJECT
CREATE TABLE IF NOT EXISTS encoded_rpki_object (
    ero_id BIGINT AUTO_INCREMENT,
    rpo_id BIGINT,
    ero_encoded BINARY NOT NULL,
    PRIMARY KEY (ero_id),
    UNIQUE (rpo_id ASC),
    FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE
);


-- Table VALIDATION_RUN
CREATE TABLE IF NOT EXISTS validation_run (
    var_id BIGINT DEFAULT seq_validation_run.nextval,
    var_updated_at VARCHAR NOT NULL,
    var_completed_at VARCHAR,
    var_status VARCHAR(20) NOT NULL,
    var_type VARCHAR(20) NOT NULL,
    tal_id INTEGER,
    var_tal_certificate_uri VARCHAR,
    PRIMARY KEY (var_id),
    FOREIGN KEY (tal_id) REFERENCES tal (tal_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS validation_run__trust_anchor_id_idx ON validation_run (tal_id ASC);


-- Table VALIDATION_CHECK
CREATE TABLE IF NOT EXISTS validation_check (
    vac_id BIGINT DEFAULT seq_validation_check.nextval,
    var_id BIGINT,
    vac_location VARCHAR(400) NOT NULL,
    vac_file_type VARCHAR(50) NOT NULL,
    vac_status VARCHAR(30) NOT NULL,
    vac_key VARCHAR(100),
    PRIMARY KEY (vac_id),
    FOREIGN KEY (var_id) REFERENCES validation_run (var_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS validation_check__validation_run_id_idx ON validation_check (var_id ASC, vac_id ASC);


--Table VALIDATION_CHECK_PARAMETERS
CREATE TABLE IF NOT EXISTS validation_check_parameters (
    vac_id BIGINT,
    vcp_id BIGINT,
    vcp_parameters VARCHAR(100) NOT NULL,
    PRIMARY KEY (vac_id, vcp_id),
    FOREIGN KEY (vac_id) REFERENCES validation_check (vac_id) ON DELETE CASCADE
);


-- Table VALIDATION_RUN_RPKI_REPOSITORIES
CREATE TABLE IF NOT EXISTS validation_run_rpki_repositories (
    var_id BIGINT,
    rpr_id BIGINT,
    PRIMARY KEY (var_id, rpr_id),
    FOREIGN KEY (var_id) REFERENCES validation_run (var_id) ON DELETE CASCADE,
    FOREIGN KEY (rpr_id) REFERENCES rpki_repository (rpr_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS validation_run_rpki_repositories__rpki_repository_idx ON validation_run_rpki_repositories (rpr_id);


-- Table ROA
CREATE TABLE IF NOT EXISTS roa (
  rpo_id BIGINT,
  roa_id BIGINT AUTO_INCREMENT,
  roa_asn BIGINT NOT NULL,
  roa_prefix_text VARCHAR(50) NOT NULL,
  roa_start_prefix BINARY NOT NULL,
  roa_end_prefix BINARY NOT NULL,
  roa_prefix_length INTEGER NOT NULL,
  roa_prefix_max_length INTEGER NOT NULL,
  roa_prefix_family INTEGER NOT NULL CHECK (roa_prefix_family IN (4, 6)),
  PRIMARY KEY (roa_id),
  FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS roa_start_prefix_idx ON roa (roa_start_prefix ASC);
CREATE INDEX IF NOT EXISTS roa_end_prefix_idx ON roa (roa_end_prefix ASC);


-- Table GBR
CREATE TABLE IF NOT EXISTS gbr (
  gbr_id BIGINT AUTO_INCREMENT,
  rpo_id BIGINT NOT NULL,
  gbr_vcard VARCHAR NOT NULL,
  PRIMARY KEY (gbr_id),
  FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE);


-- Table SLURM_PREFIX
CREATE TABLE IF NOT EXISTS slurm_prefix (
  slp_id BIGINT AUTO_INCREMENT,
  slp_asn BIGINT NULL,
  slp_prefix_text VARCHAR(50) NULL,
  slp_start_prefix BINARY NULL,
  slp_end_prefix BINARY NULL,
  slp_prefix_length INTEGER NULL,
  slp_prefix_max_length INTEGER NULL,
  slp_type VARCHAR NOT NULL,
  slp_comment VARCHAR NULL,
  slp_order INTEGER NULL,
  PRIMARY KEY (slp_id));

CREATE INDEX IF NOT EXISTS slp_start_prefix_idx ON slurm_prefix (slp_start_prefix ASC);
CREATE INDEX IF NOT EXISTS slp_end_prefix_idx ON slurm_prefix (slp_end_prefix ASC);


-- Table SLURM_BGPSEC
CREATE TABLE IF NOT EXISTS slurm_bgpsec (
  slb_id BIGINT AUTO_INCREMENT,
  slb_asn BIGINT NULL,
  slb_ski VARCHAR NULL,
  slb_public_key VARCHAR NULL,
  slb_type VARCHAR NOT NULL,
  slb_comment VARCHAR NULL,
  slb_order INTEGER NULL,
  PRIMARY KEY (slb_id));


-- Table SLURM_CHECKSUM
CREATE TABLE IF NOT EXISTS slurm_checksum (
  sch_checksum BINARY);

-- Insert null value only if there's already no value
insert into slurm_checksum(sch_checksum)
select null
  from (select count(*) count from slurm_checksum) c
 where c.count = 0;