#getById
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment,
       slp_order
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
       slp_comment,
       slp_order
  from slurm_prefix
 where 1 = 1
[filter]
[order]
[limit];

#getAllCount
select count(*)
  from slurm_prefix
 where 1 = 1
[filter];

#getAllByType
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment,
       slp_order
  from slurm_prefix
 where slp_type = ?
[filter]
[order]
[limit];

#getAllByTypeCount
select count(*)
  from slurm_prefix
 where slp_type = ?
[filter];

#exist
select 1
  from slurm_prefix
 where slp_type = ?
 [and];

#create
insert into slurm_prefix (
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment,
       slp_order)
values (?, ?, ?, ?, ?, ?, ?, ?, ?);

#deleteById
delete from slurm_prefix where slp_id = ?;

#deleteAll
delete from slurm_prefix;

#getByProperties
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment,
       slp_order
  from slurm_prefix
 where slp_type = ?
 [and];

#updateComment
update slurm_prefix
   set slp_comment = ?
 where slp_id = ?;

#updateOrder
update slurm_prefix
   set slp_order = ?
 where slp_id = ?;

#findExactMatch
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment,
       slp_order
  from slurm_prefix
 where slp_type = ?
   and ? between slp_start_prefix and slp_end_prefix
   and slp_prefix_length <= ?
   and ifnull(slp_prefix_max_length, slp_prefix_length) >= ?
 order by slp_start_prefix desc, slp_prefix_length desc;

#findCoveringAggregate
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment,
       slp_order
  from slurm_prefix
 where slp_type = ?
   and slp_start_prefix <= ?
   and ifnull(slp_prefix_max_length, slp_prefix_length) < ?
 order by slp_start_prefix desc, slp_prefix_length desc;

#findMoreSpecific
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment,
       slp_order
  from slurm_prefix
 where slp_type = ?
   and slp_start_prefix >= ?
   and slp_prefix_length > ?
 order by slp_start_prefix, slp_prefix_length;

#findFilterMatch
select slp_id,
       slp_asn,
       slp_prefix_text,
       slp_start_prefix,
       slp_end_prefix,
       slp_prefix_length,
       slp_prefix_max_length,
       slp_type,
       slp_comment,
       slp_order
  from slurm_prefix
 where slp_type = ? 
   and ((slp_start_prefix is null and slp_asn = ?)
    or  (slp_asn is null and ? between slp_start_prefix and slp_end_prefix and slp_prefix_length <= ?)
    or  (slp_asn = ? and ? between slp_start_prefix and slp_end_prefix and slp_prefix_length <= ?))
 order by slp_start_prefix desc, slp_prefix_length desc, slp_asn desc;