#getById
select tal_id,
       tal_last_sync,
       tal_public_key,
       tal_sync_status,
       tal_validation_status,
       tal_name,
       tal_loaded_cer
  from tal
 where tal_id = ?;

#getAll
select tal_id,
       tal_last_sync,
       tal_public_key,
       tal_sync_status,
       tal_validation_status,
       tal_name,
       tal_loaded_cer
  from tal
[order]
[limit];

#syncById
update tal
   set tal_sync_status = "synchronizing"
 where tal_id = ?;

#getLastId
select max(tal_id)
  from tal;

#exist
select 1
  from tal
 where 1 = 1
 [and];

#create
insert into tal (
       tal_id,
       tal_last_sync,
       tal_public_key,
       tal_sync_status,
       tal_validation_status,
       tal_name,
       tal_loaded_cer)
values (?, ?, ?, ?, ?, ?, ?);

#delete
delete from tal where tal_id = ?;

#getByRpkiRepositoryId
select t.tal_id,
       t.tal_last_sync,
       t.tal_public_key,
       t.tal_sync_status,
       t.tal_validation_status,
       t.tal_name,
       t.tal_loaded_cer
  from tal t
  join rpki_repository_trust_anchors r on r.tal_id = t.tal_id
 where r.rpr_id = ?;

#updateLoadedCer
update tal
   set tal_loaded_cer = ?
 where tal_id = ?;