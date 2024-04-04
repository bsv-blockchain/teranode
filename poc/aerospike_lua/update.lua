-- The first argument is the record to update. This is passed to the UDF by aerospike based on the Key that the UDF is getting executed on
-- The second argument is the value to update the bin with
function updateRecord(rec, val)
  if not aerospike:exists(rec) then
    return "Record doesn't exist"
  end

  rec["val"] = val
  aerospike:update(rec)
  return "OK"
end

