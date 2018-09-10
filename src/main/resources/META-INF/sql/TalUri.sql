#getByTalId
select tau_id, tal_id, tau_location
  from tal_uri
 where tal_id = ?
 order by tau_id;

#getLastId
select max(tau_id)
  from tal_uri;

#create
insert into tal_uri (tau_id, tal_id, tau_location)
values (?, ?, ?);