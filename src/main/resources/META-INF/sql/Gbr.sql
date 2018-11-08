#getByParentCa
select g.gbr_id, g.rpo_id, g.gbr_vcard
  from gbr g
  join rpki_object r on r.rpo_id = g.rpo_id
 where r.rpo_authority_key_identifier = ?
   and r.rpo_type = ?
 order by g.gbr_id;

#create
insert into gbr (rpo_id, gbr_vcard)
values (?, ?);

#getByRpkiObjectId
select gbr_id, rpo_id, gbr_vcard
  from gbr
 where rpo_id = ?;