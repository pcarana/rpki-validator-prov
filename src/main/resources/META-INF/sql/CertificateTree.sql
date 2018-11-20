#geFromRoot
select rpo_id, rpo_type, rpo_subject_key_identifier
  from rpki_object
 where rpo_authority_key_identifier = ?
   and ifnull(rpo_subject_key_identifier, '') != rpo_authority_key_identifier;

#countChilds
select count(*)
  from rpki_object
 where rpo_authority_key_identifier = ?
   and ifnull(rpo_subject_key_identifier, '') != rpo_authority_key_identifier;