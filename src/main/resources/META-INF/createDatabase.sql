-- Table TAL
CREATE TABLE IF NOT EXISTS tal (
  tal_id INTEGER,
  tal_public_key TEXT NULL,
  tal_name TEXT NULL,
  tal_loaded_cer BLOB NULL,
  PRIMARY KEY (tal_id));


-- Table TAL_URI
CREATE TABLE IF NOT EXISTS tal_uri (
  tau_id INTEGER,
  tal_id INTEGER,
  tau_location TEXT NOT NULL,
  PRIMARY KEY (tau_id),
  FOREIGN KEY (tal_id) REFERENCES TAL (tal_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS tal_uri_tal_id_idx ON TAL_URI (tal_id ASC);


-- Table RPKI_REPOSITORY
CREATE TABLE IF NOT EXISTS rpki_repository (
    rpr_id INTEGER,
    rpr_updated_at TEXT NOT NULL,
    rpr_location_uri TEXT,
    rpr_parent_repository_id INTEGER,
    PRIMARY KEY (rpr_id),
    UNIQUE (rpr_location_uri),
    FOREIGN KEY (rpr_parent_repository_id) REFERENCES rpki_repository (rpr_id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS rpki_repository__parent_repository_id_idx ON rpki_repository (rpr_parent_repository_id ASC);


-- Table RPKI_REPOSITORY_TRUST_ANCHORS
CREATE TABLE IF NOT EXISTS rpki_repository_trust_anchors (
    rpr_id INTEGER,
    tal_id INTEGER,
    PRIMARY KEY (rpr_id, tal_id),
    FOREIGN KEY (tal_id) REFERENCES tal (tal_id) ON DELETE CASCADE,
    FOREIGN KEY (rpr_id) REFERENCES rpki_repository (rpr_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS rpki_repository_trust_anchors__trust_anchor_id_idx ON rpki_repository_trust_anchors (tal_id ASC);


-- Table RPKI_OBJECT
CREATE TABLE IF NOT EXISTS rpki_object (
    rpo_id INTEGER,
    rpo_type TEXT NOT NULL,
    rpo_serial_number BLOB,
    rpo_signing_time INTEGER,
    rpo_last_marked_reachable_at TEXT NOT NULL,
    rpo_authority_key_identifier BLOB,
    rpo_subject_key_identifier BLOB,
    rpo_sha256 BLOB NOT NULL,
    rpo_is_ca INTEGER NOT NULL,
    PRIMARY KEY (rpo_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS rpki_object__sha256_idx ON rpki_object (rpo_sha256 ASC);
CREATE INDEX IF NOT EXISTS rpki_object__authority_key_identifier_idx ON rpki_object (rpo_authority_key_identifier ASC, rpo_type ASC, rpo_serial_number DESC, rpo_signing_time DESC, rpo_id DESC);
CREATE INDEX IF NOT EXISTS rpki_object__subject_key_identifier_idx ON rpki_object (rpo_subject_key_identifier ASC);


-- Table RPKI_OBJECT_LOCATIONS
CREATE TABLE IF NOT EXISTS rpki_object_locations (
    rpo_id INTEGER,
    rpo_locations TEXT NOT NULL,
    PRIMARY KEY (rpo_id, rpo_locations),
    FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE
);


-- Table RPKI_REPOSITORY_RPKI_OBJECT
CREATE TABLE IF NOT EXISTS rpki_repository_rpki_object (
    rpr_id INTEGER,
    rpo_id INTEGER,
    PRIMARY KEY (rpr_id, rpo_id),
    FOREIGN KEY (rpr_id) REFERENCES rpki_repository (rpr_id) ON DELETE CASCADE,
    FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE
);


-- Table ENCODED_RPKI_OBJECT
CREATE TABLE IF NOT EXISTS encoded_rpki_object (
    ero_id INTEGER,
    rpo_id INTEGER,
    ero_encoded BLOB NOT NULL,
    PRIMARY KEY (ero_id),
    UNIQUE (rpo_id ASC),
    FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE
);


-- Table VALIDATION_RUN
CREATE TABLE IF NOT EXISTS validation_run (
    var_id INTEGER,
    var_updated_at TEXT NOT NULL,
    var_completed_at TEXT,
    var_status TEXT NOT NULL,
    var_type TEXT NOT NULL,
    tal_id INTEGER,
    var_tal_certificate_uri TEXT,
    PRIMARY KEY (var_id),
    FOREIGN KEY (tal_id) REFERENCES tal (tal_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS validation_run__trust_anchor_id_idx ON validation_run (tal_id ASC);


-- Table VALIDATION_CHECK
CREATE TABLE IF NOT EXISTS validation_check (
    vac_id INTEGER,
    var_id INTEGER,
    vac_location TEXT NOT NULL,
    vac_status TEXT NOT NULL,
    vac_key TEXT,
    PRIMARY KEY (vac_id),
    FOREIGN KEY (var_id) REFERENCES validation_run (var_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS validation_check__validation_run_id_idx ON validation_check (var_id ASC, vac_id ASC);


--Table VALIDATION_CHECK_PARAMETERS
CREATE TABLE IF NOT EXISTS validation_check_parameters (
    vac_id INTEGER,
    vcp_id INTEGER,
    vcp_parameters TEXT NOT NULL,
    PRIMARY KEY (vac_id, vcp_id),
    FOREIGN KEY (vac_id) REFERENCES validation_check (vac_id) ON DELETE CASCADE
);


-- Table VALIDATION_RUN_RPKI_REPOSITORIES
CREATE TABLE IF NOT EXISTS validation_run_rpki_repositories (
    var_id INTEGER,
    rpr_id INTEGER,
    PRIMARY KEY (var_id, rpr_id),
    FOREIGN KEY (var_id) REFERENCES validation_run (var_id) ON DELETE CASCADE,
    FOREIGN KEY (rpr_id) REFERENCES rpki_repository (rpr_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS validation_run_rpki_repositories__rpki_repository_idx ON validation_run_rpki_repositories (rpr_id);


-- Table ROA
CREATE TABLE IF NOT EXISTS roa (
  rpo_id INTEGER,
  roa_id INTEGER,
  roa_asn INTEGER NOT NULL,
  roa_prefix_text TEXT NOT NULL,
  roa_start_prefix BLOB NOT NULL,
  roa_end_prefix BLOB NOT NULL,
  roa_prefix_length INTEGER NOT NULL,
  roa_prefix_max_length INTEGER NOT NULL,
  roa_prefix_family INTEGER NOT NULL CHECK (roa_prefix_family IN (4, 6)),
  PRIMARY KEY (roa_id),
  FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS start_prefix_idx ON roa (roa_start_prefix ASC);
CREATE INDEX IF NOT EXISTS end_prefix_idx ON roa (roa_end_prefix ASC);


-- Table GBR
CREATE TABLE IF NOT EXISTS gbr (
  gbr_id INTEGER,
  rpo_id INTEGER NOT NULL,
  gbr_vcard TEXT NOT NULL,
  PRIMARY KEY (gbr_id),
  FOREIGN KEY (rpo_id) REFERENCES rpki_object (rpo_id) ON DELETE CASCADE);


-- Table SLURM_PREFIX
CREATE TABLE IF NOT EXISTS slurm_prefix (
  slp_id INTEGER,
  slp_asn INTEGER NULL,
  slp_prefix_text TEXT NULL,
  slp_start_prefix BLOB NULL,
  slp_end_prefix BLOB NULL,
  slp_prefix_length INTEGER NULL,
  slp_prefix_max_length INTEGER NULL,
  slp_type TEXT NOT NULL,
  slp_comment TEXT NULL,
  PRIMARY KEY (slp_id));


-- Table SLURM_BGPSEC
CREATE TABLE IF NOT EXISTS slurm_bgpsec (
  slb_id INTEGER,
  slb_asn INTEGER NULL,
  slb_ski TEXT NULL,
  slb_public_key TEXT NULL,
  slb_type TEXT NOT NULL,
  slb_comment TEXT NULL,
  PRIMARY KEY (slb_id));
