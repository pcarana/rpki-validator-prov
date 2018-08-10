#getAll
select rts_id,
       rts_address,
       rts_port,
       rts_status,
       rts_last_request,
       rts_last_response,
       rts_session_id,
       rts_serial_number,
       rts_version
  from rtr_session
[order]
[limit];