#getByValidationRunId
select vac_id,
       vac_updated_at,
       var_id,
       vac_location,
       vac_status,
       vac_key
  from validation_check
 where var_id = ?;

#getParameters
select vcp_parameters
  from validation_check
 where vac_id = ?;

#getLastId
select max(vac_id)
  from validation_check;

#getLastParameterId
select max(vcp_id)
  from validation_check_parameters
 where vac_id = ?;

#create
insert into validation_check (
       vac_id,
       vac_updated_at,
       var_id,
       vac_location,
       vac_status,
       vac_key)
values (?, ?, ?, ?, ?, ?);

#createParameter
insert into validation_check_parameters (
       vac_id,
       vcp_id,
       vcp_parameters)
values (?, ?, ?);