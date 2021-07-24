## Data structure

### Block

**key**|**type**
:-----:|:-----:
block\_size| bigint
cumulative\_diff| text
diff| bigint
hash| text
hash\_list\_merkle| text
height| bigint
indep\_hash| text
last\_retarget| bigint
nonce| text
previous\_block| text
reward\_addr| text
reward\_pool| bigint
tags| list
timestamp| bigint
tx\_root| text
tx\_tree| frozen
txs| frozen
wallet\_list| text
weave\_size| bigint

## With Docker

<kbd>docker run --name cassandra --network bridge -p 9042:9042 cassandra:4.0-rc2</kbd>


## More

https://ralph.blog.imixs.com/2020/06/22/setup-a-public-cassandra-cluster-with-docker/
