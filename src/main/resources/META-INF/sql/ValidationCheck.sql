#getByValidationRunId
select vac_id,
       var_id,
       vac_location,
       vac_file_type,
       vac_status,
       vac_key
  from validation_check
 where var_id = ?;

#getParameters
select vcp_parameters
  from validation_check_parameters
 where vac_id = ?;

#getLastParameterId
select max(vcp_id)
  from validation_check_parameters
 where vac_id = ?;

#create
insert into validation_check (
       var_id,
       vac_location,
       vac_file_type,
       vac_status,
       vac_key)
values (?, ?, ?, ?, ?);

#createParameter
insert into validation_check_parameters (
       vac_id,
       vcp_id,
       vcp_parameters)
values (?, ?, ?);

#getLastSuccessfulChecksByTal
select vc.vac_id,
       vc.var_id,
       vc.vac_location,
       vc.vac_file_type,
       vc.vac_status,
       vc.vac_key
  from validation_check vc
  join validation_run vr on vr.var_id = vc.var_id
 where vr.tal_id = ?
   and vr.var_status = 'SUCCEEDED'
   and vr.var_id = (
        select var_id
          from validation_run
         where tal_id = vr.tal_id
           and var_status = 'SUCCEEDED'
         order by var_completed_at desc
         limit 1)
[filter]
[order]
[limit];

#getLastSuccessfulChecksByTalCount
select count(*)
  from validation_check vc
  join validation_run vr on vr.var_id = vc.var_id
 where vr.tal_id = ?
   and vr.var_status = 'SUCCEEDED'
   and vr.var_id = (
        select var_id
          from validation_run
         where tal_id = vr.tal_id
           and var_status = 'SUCCEEDED'
         order by var_completed_at desc
         limit 1)
[filter];

#getLastSuccessfulChecksSummByTal
select vc.vac_status, vc.vac_file_type, count(*)
  from validation_check vc
  join validation_run vr on vr.var_id = vc.var_id
 where vr.tal_id = ?
   and vr.var_status = 'SUCCEEDED'
   and vr.var_id = (
        select var_id
          from validation_run
         where tal_id = vr.tal_id
           and var_status = 'SUCCEEDED'
         order by var_completed_at desc
         limit 1)
 group by vc.vac_status, vc.vac_file_type
 order by 1, 2;

#getLastRowid
select seq_validation_check.currval;