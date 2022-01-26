SELECT cf.numberx
FROM special_facility AS sf,
     call_forwarding AS cf
WHERE sf.s_id = ?
  AND sf.sf_type = ?
  AND sf.is_active = 1
  AND cf.s_id = sf.s_id
  AND cf.sf_type = sf.sf_type
  AND cf.start_time <= ?
  AND ? < cf.end_time;
