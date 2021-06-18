# openwsn-data

This repository helps to analyse the data produced by openwsn-fw (https://github.com/ftheoleyre/openwsn-fw/tree/CCA) and openvisualizer (https://github.com/ftheoleyre/openvisualizer).

# Data

the directory `results/*` should contain the raw logs of an openwsn+openvisualizer instance (logs + db file).

The current python script only handles the db (sqlite3) `openv_events.db`, that stores everything event in a separated table (pkt, application, queue, etc.)



# Topology

It extracts:

* a list of motes
* a list of links (at least one packet txed)



# Traffic

The script:

* uses an sqlite db file as input `--db filename.db`
* identifies the Cexample packets 
* for each packet, keep a record of each action all along the path (tx, asn, etc.)
* computes the end-to-end PDR and a bunch of different statistics