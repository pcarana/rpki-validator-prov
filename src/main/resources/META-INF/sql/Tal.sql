#getById
select tal_id,
       tal_public_key,
       tal_name,
       tal_loaded_cer
  from tal
 where tal_id = ?;

#getAll
select tal_id,
       tal_public_key,
       tal_name,
       tal_loaded_cer
  from tal
[order]
[limit];

#getAllCount
select count(*)
  from tal;

#exist
select 1
  from tal
 where 1 = 1
 [and];

#create
insert into tal (
       tal_public_key,
       tal_name,
       tal_loaded_cer)
values (?, ?, ?);

#delete
delete from tal where tal_id = ?;

#getByRpkiRepositoryId
select t.tal_id,
       t.tal_public_key,
       t.tal_name,
       t.tal_loaded_cer
  from tal t
  join rpki_repository_trust_anchors r on r.tal_id = t.tal_id
 where r.rpr_id = ?;

#getByUnique
select tal_id,
       tal_public_key,
       tal_name,
       tal_loaded_cer
  from tal
 where 1 = 1
 [and]
 order by tal_id desc;

#updateLoadedCer
update tal
   set tal_loaded_cer = ?
 where tal_id = ?;