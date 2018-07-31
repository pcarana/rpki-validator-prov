#getById
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment
  from slurm_prefix
 where slp_id = ?;

#getAll
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment
  from slurm_prefix
 order by slp_id;

#getAllByType
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment
  from slurm_prefix
 where slp_type = ?
 order by slp_id;

#getLastId
select max(slp_id)
  from slurm_prefix;

#exist
select 1
  from slurm_prefix
 where slp_type = ?
 [where];

#create
insert into slurm_prefix (
       slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment)
values (?, ?, ?, ?, ?, ?, ?, ?, ?);

#deleteById
delete from slurm_prefix where slp_id = ?;