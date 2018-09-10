#getBy
select rpo_id,
       rpo_updated_at,
       rpo_type,
       rpo_serial_number,
       rpo_signing_time,
       rpo_last_marked_reachable_at,
       rpo_authority_key_identifier,
       rpo_sha256
  from rpki_object
 where 1 = 1
 [and]
 [order]
 [limit];

#getLocations
select rpo_locations
  from rpki_object_locations
 where rpo_id = ?;

#getEncodedByRpkiObjectId
select ero_id,
       ero_updated_at,
       ero_encoded
  from encoded_rpki_object
 where rpo_id = ?;

#getValidatedByValidationRunId
select r.rpo_id,
       r.rpo_updated_at,
       r.rpo_type,
       r.rpo_serial_number,
       r.rpo_signing_time,
       r.rpo_last_marked_reachable_at,
       r.rpo_authority_key_identifier,
       r.rpo_sha256
  from rpki_object r
  join validation_run_validated_objects v on v.rpo_id = r.rpo_id
 where r.var_id = ?;

#getLastId
select max(rpo_id)
  from rpki_object;

#exist
select 1
  from rpki_object
 where 1 = 1
 [and];

#create
insert into rpki_object (
       rpo_id,
       rpo_updated_at,
       rpo_type,
       rpo_serial_number,
       rpo_signing_time,
       rpo_last_marked_reachable_at,
       rpo_authority_key_identifier,
       rpo_sha256)
values (?, ?, ?, ?, ?, ?, ?, ?);

#delete
delete from rpki_object where rpo_id = ?;

#deleteUnreachable
delete from rpki_object where rpo_last_marked_reachable_at < ?;

#createEncodedRpkiObject
insert into encoded_rpki_object (
       ero_id,
       ero_updated_at,
       rpo_id,
       ero_encoded)
values (?, ?, ?, ?);

#getEncodedRpkiObjectLastId
select max(ero_id)
  from encoded_rpki_object;

#createLocation
insert into rpki_object_locations (rpo_id, rpo_locations)
values (?, ?);