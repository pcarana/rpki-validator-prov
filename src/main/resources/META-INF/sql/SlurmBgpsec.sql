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
 order by slb_id;

#getAllByType
select slb_id,
       slb_asn,
       slb_ski,
       slb_public_key,
       slb_type,
       slb_comment
  from slurm_bgpsec
 where slb_type = ?
 order by slb_id;