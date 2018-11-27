#getById
select slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment,
       slb_order
  from slurm_bgpsec
 where slb_id = ?;

#getAll
select slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment,
       slb_order
  from slurm_bgpsec
[order]
[limit];

#getAllCount
select count(*)
  from slurm_bgpsec;

#getAllByType
select slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment,
       slb_order
  from slurm_bgpsec
 where slb_type = ?
[order]
[limit];

#getAllByTypeCount
select count(*)
  from slurm_bgpsec
 where slb_type = ?;

#exist
select 1
  from slurm_bgpsec
 where slb_type = ?
 [and];

#create
insert into slurm_bgpsec (
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment,
       slb_order)
values (?, ?, ?, ?, ?, ?);

#deleteById
delete from slurm_bgpsec where slb_id = ?;

#deleteAll
delete from slurm_bgpsec;

#getByProperties
select slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment,
       slb_order
  from slurm_bgpsec
 where slb_type = ?
 [and];

#updateComment
update slurm_bgpsec
   set slb_comment = ?
 where slb_id = ?;

#updateOrder
update slurm_bgpsec
   set slb_order = ?
 where slb_id = ?;