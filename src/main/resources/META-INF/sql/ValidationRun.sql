#getAll
select var_id,
       var_updated_at,
       var_completed_at,
       var_status,
       var_type,
       tal_id,
       var_tal_certificate_uri
  from validation_run
  [order]
  [limit];

#create
insert into validation_run (
       var_updated_at,
       var_completed_at,
       var_status,
       var_type,
       tal_id,
       var_tal_certificate_uri)
values (?, ?, ?, ?, ?, ?);

#update
update validation_run
   set var_updated_at = ?,
       var_completed_at = ?,
       var_status = ?,
       var_type = ?,
       tal_id = ?,
       var_tal_certificate_uri = ?
 where var_id = ?;

#createRepositoryRelation
insert into validation_run_rpki_repositories (var_id, rpr_id)
values (?, ?);

#createValidObjectsRelation
insert into validation_run_validated_objects (var_id, rpo_id)
values (?, ?);

#deleteOld
delete from validation_run
 where var_completed_at < ?
   and var_status != 'RUNNING';

#getByUnique
select var_id,
       var_updated_at,
       var_completed_at,
       var_status,
       var_type,
       tal_id,
       var_tal_certificate_uri
  from validation_run
 where 1 = 1
 [and]
 order by var_id desc;

#getByTalId
select var_id,
       var_updated_at,
       var_completed_at,
       var_status,
       var_type,
       tal_id,
       var_tal_certificate_uri
  from validation_run
 where tal_id = ?
 order by var_id desc;