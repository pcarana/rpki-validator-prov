#getById
select rpo_id,
       roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_prefix_family
  from roa
 where roa_id = ?;

#getAll
select rpo_id,
       roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_prefix_family
  from roa
[order]
[limit];

#getAllCount
select count(*)
  from roa;

#findExactMatch
select rpo_id,
       roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_prefix_family
  from roa
 where ? between roa_start_prefix and roa_end_prefix
   and roa_prefix_length <= ?
   and roa_prefix_max_length >= ?
 order by roa_start_prefix desc, roa_prefix_length desc;

#findCoveringAggregate
select rpo_id,
       roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_prefix_family
  from roa
 where roa_start_prefix <= ?
   and roa_prefix_max_length < ?
 order by roa_start_prefix desc, roa_prefix_length desc;

#findMoreSpecific
select rpo_id,
       roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_prefix_family
  from roa
 where roa_start_prefix >= ?
   and roa_prefix_length > ?
 order by roa_start_prefix, roa_prefix_length;

#existAsn
select 1
  from roa
 where roa_asn = ?;

#getByRpkiObjectId
select rpo_id,
       roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_prefix_family
  from roa
 where rpo_id = ?
 order by roa_start_prefix, roa_prefix_length;

#getLastId
select max(roa_id)
  from roa;

#create
insert into roa (
       rpo_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_prefix_family)
values (?, ?, ?, ?, ?, ?, ?, ?);