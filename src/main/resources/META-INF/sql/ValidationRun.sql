#getAll
select var_id,
       var_updated_at,
       var_completed_at,
       var_status,
       tal_id,
       var_tal_certificate_uri
  from validation_run
  [order]
  [limit];

#getLastId
select max(var_id)
  from validation_run;

#exist
select 1
  from validation_run
 where 1 = 1
 [and];

#create
insert into validation_run (
       var_id,
       var_updated_at,
       var_completed_at,
       var_status,
       tal_id,
       var_tal_certificate_uri)
values (?, ?, ?, ?, ?, ?);

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