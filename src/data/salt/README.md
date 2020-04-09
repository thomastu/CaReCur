# Overview

The purpose of this directory is to quickly provision manually created worker instances.  This is done through a masterless salt setup.

Install the salt-minion:

```
# Follow Salt-masterless bootstrap:
# https://docs.saltstack.com/en/latest/topics/tutorials/quickstart.html#bootstrap-salt-minion

curl -L https://bootstrap.saltstack.com -o bootstrap_salt.sh
sudo sh bootstrap_salt.sh
```

```
# Apply state
sudo salt-call --config-dir=./src/data/salt --file-root=./src/data/salt --local state.apply
```