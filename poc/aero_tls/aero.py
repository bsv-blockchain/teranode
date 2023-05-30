import aerospike

user = "read-write"
password = "i23nqwreak"
config = {'hosts': [('aerospike.aerospike.svc.cluster.local', 3000)], 'policies': {'timeout': 1000}, 'user': user,'password': password}

client = aerospike.client(config).connect()

write_policy = {'key': aerospike.POLICY_KEY_SEND}

key = ('test', 'my_set', "person")
value = {"name":"joe"}

client.put(key,value, policy=write_policy)

(key, meta) = client.exists(key)
print(key, meta)
