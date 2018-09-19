#getBy
select rpo_id,
       rpo_type,
       rpo_serial_number,
       rpo_signing_time,
       rpo_last_marked_reachable_at,
       rpo_authority_key_identifier,
       rpo_subject_key_identifier,
       rpo_sha256,
       rpo_is_ca
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
       rpo_id,
       ero_encoded
  from encoded_rpki_object
 where rpo_id = ?;

#getRpkiRepositoryRelation
select rpr_id, rpo_id
  from rpki_repository_rpki_object
 where rpo_id = ?;

#exist
select 1
  from rpki_object
 where 1 = 1
 [and];

#create
insert into rpki_object (
       rpo_type,
       rpo_serial_number,
       rpo_signing_time,
       rpo_last_marked_reachable_at,
       rpo_authority_key_identifier,
       rpo_subject_key_identifier,
       rpo_sha256,
       rpo_is_ca)
values (?, ?, ?, ?, ?, ?, ?, ?);

#delete
delete from rpki_object where rpo_id = ?;

#deleteUnreachable
delete from rpki_object where rpo_last_marked_reachable_at < ?;

#createEncodedRpkiObject
insert into encoded_rpki_object (
       ero_id,
       rpo_id,
       ero_encoded)
values (?, ?, ?);

#getEncodedRpkiObjectLastId
select max(ero_id)
  from encoded_rpki_object;

#createLocation
insert into rpki_object_locations (rpo_id, rpo_locations)
values (?, ?);

#createRpkiRepositoryRelation
insert into rpki_repository_rpki_object (rpr_id, rpo_id)
values (?, ?);

#deleteByRpkiRepositoryId
delete from rpki_object
 where rpo_id in (
   select rpo_id
     from rpki_repository_rpki_object
    where rpr_id = ?);