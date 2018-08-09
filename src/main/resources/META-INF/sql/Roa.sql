#getById
select roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_cms_data,
       tal_id
  from roa
 where roa_id = ?;

#getAll
select roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_cms_data,
       tal_id
  from roa
 order by roa_id;

#findExactMatch
select roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_cms_data,
       tal_id
  from roa
 where ? between roa_start_prefix and roa_end_prefix
   and roa_prefix_length <= ?
   and roa_prefix_max_length >= ?
 order by roa_start_prefix desc, roa_prefix_length desc;

#findCoveringAggregate
select roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_cms_data,
       tal_id
  from roa
 where roa_start_prefix <= ?
   and roa_prefix_max_length < ?
 order by roa_start_prefix desc, roa_prefix_length desc;

#findMoreSpecific
select roa_id,
       roa_asn,
       roa_prefix_text,
       roa_start_prefix,
       roa_end_prefix,
       roa_prefix_length,
       roa_prefix_max_length,
       roa_cms_data,
       tal_id
  from roa
 where roa_start_prefix >= ?
   and roa_prefix_length > ?
 order by roa_start_prefix, roa_prefix_length;

#existAsn
select 1
  from roa
 where roa_asn = ?;