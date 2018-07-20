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