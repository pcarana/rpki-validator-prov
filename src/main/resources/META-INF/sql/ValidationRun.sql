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

#deleteOld
delete from validation_run
 where var_id < ?
   and var_type = ?
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
union
select vr.var_id,
       vr.var_updated_at,
       vr.var_completed_at,
       vr.var_status,
       vr.var_type,
       vr.tal_id,
       vr.var_tal_certificate_uri
  from validation_run vr
  join validation_run_rpki_repositories vrp on vrp.var_id = vr.var_id
  join rpki_repository rr on rr.rpr_id = vrp.rpr_id
  join rpki_repository_trust_anchors rpt on rpt.rpr_id = rr.rpr_id
 where vr.var_type = 'RPKI_REPOSITORY'
   and rpt.tal_id = ?
 order by var_id desc;