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

#deleteOld
delete from validation_run
 where var_id < ?
   and var_type = ?
   and tal_id = ?
   and var_status != 'RUNNING';

#getById
select var_id,
       var_updated_at,
       var_completed_at,
       var_status,
       var_type,
       tal_id,
       var_tal_certificate_uri
  from validation_run
 where var_id = ?;

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

#getLastRowid
select seq_validation_run.currval;