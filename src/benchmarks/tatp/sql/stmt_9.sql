DELETE
FROM call_forwarding
WHERE s_id = ?
  AND sf_type = ?
  AND start_time = ?;
