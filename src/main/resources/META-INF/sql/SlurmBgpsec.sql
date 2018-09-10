#getById
select slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment
  from slurm_bgpsec
 where slb_id = ?;

#getAll
select slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment
  from slurm_bgpsec
[order]
[limit];

#getAllByType
select slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment
  from slurm_bgpsec
 where slb_type = ?
[order]
[limit];

#getLastId
select max(slb_id)
  from slurm_bgpsec;

#exist
select 1
  from slurm_bgpsec
 where slb_type = ?
 [and];

#create
insert into slurm_bgpsec (
       slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment)
values (?, ?, ?, ?, ?, ?);

#deleteById
delete from slurm_bgpsec where slb_id = ?;