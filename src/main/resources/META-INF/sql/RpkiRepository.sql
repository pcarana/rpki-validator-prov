#create
insert into rpki_repository (
       rpr_updated_at,
       rpr_location_uri,
       rpr_parent_repository_id)
values (?, ?, ?);

#getById
select rpr_id,
       rpr_updated_at,
       rpr_location_uri,
       rpr_parent_repository_id
  from rpki_repository
 where rpr_id = ?;

#getByUri
select rpr_id,
       rpr_updated_at,
       rpr_location_uri,
       rpr_parent_repository_id
  from rpki_repository
 where rpr_location_uri = ?;

#getByValidationRunId
select r.rpr_id,
       r.rpr_updated_at,
       r.rpr_location_uri,
       r.rpr_parent_repository_id
  from rpki_repository r
  join validation_run_rpki_repositories v on v.rpr_id = r.rpr_id
 where v.var_id = ?;

#getIdsByTalId
select rpr_id
  from rpki_repository_trust_anchors
 where tal_id = ?;

#getByTalId
select rr.rpr_id,
       rr.rpr_updated_at,
       rr.rpr_location_uri,
       rr.rpr_parent_repository_id
  from rpki_repository rr
  join rpki_repository_trust_anchors rt on rt.rpr_id = rr.rpr_id
 where rt.tal_id = ?
 order by rr.rpr_location_uri, rr.rpr_id;

#getByUnique
select rpr_id,
       rpr_updated_at,
       rpr_location_uri,
       rpr_parent_repository_id
  from rpki_repository
 where 1 = 1
 [and]
 order by rpr_id desc;

#createTalRelation
insert into rpki_repository_trust_anchors (rpr_id, tal_id)
values (?, ?);

#updateParentRepository
update rpki_repository
   set rpr_parent_repository_id = ?
 where rpr_id = ?;

#deleteByTalId
delete from rpki_repository
 where rpr_id in (
   select rpr_id
     from rpki_repository_trust_anchors
    where tal_id = ?);