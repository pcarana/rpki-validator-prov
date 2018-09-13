#getByRoaId
select g.gbr_id, g.gbr_vcard, g.rpo_id
  from gbr g
  join roa_gbr rg on rg.gbr_id = g.gbr_id
 where rg.roa_id = ?
 order by g.gbr_id;