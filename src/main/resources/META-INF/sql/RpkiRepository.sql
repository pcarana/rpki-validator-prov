#getById
select rpr_id,
       rpr_updated_at,
       rpr_status,
       rpr_last_downloaded_at,
       rpr_location_uri,
       rpr_parent_repository_id
  from rpki_repository
 where rpr_id = ?;

#getByUri
select rpr_id,
       rpr_updated_at,
       rpr_status,
       rpr_last_downloaded_at,
       rpr_location_uri,
       rpr_parent_repository_id
  from rpki_repository
 where rpr_location_uri = ?;

#getByValidationRunId
select r.rpr_id,
       r.rpr_updated_at,
       r.rpr_status,
       r.rpr_last_downloaded_at,
       r.rpr_location_uri,
       r.rpr_parent_repository_id
  from rpki_repository r
  join validation_run_rpki_repositories v on v.rpr_id = r.rpr_id
 where v.var_id = ?;

#getAll
select rpr_id,
       rpr_updated_at,
       rpr_status,
       rpr_last_downloaded_at,
       rpr_location_uri,
       rpr_parent_repository_id
  from rpki_repository
 [order]
 [limit];