-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
function setTTL(rec)
  if not aerospike:exists(rec) then
    return "Record doesn't exist"
  end

  record.set_ttl(rec, 1) -- 1 second

  local result = aerospike:update(rec)
  if ( result ~= nil and result ~= 0 ) then
    warn("expireRecord:Failed to UPDATE the record: resultCode (%s)", tostring(result))
    return "ERROR: " .. tostring(result)
  end

  return "OK"
end

