#getLastChecksum
select sch_checksum
  from slurm_checksum;

#updateLastChecksum
update slurm_checksum
   set sch_checksum = ?;