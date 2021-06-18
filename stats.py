#!/usr/bin/env python
# coding: utf-8

# systems tools
import os
import shutil
import sys
import time
import sys
import signal
import random

# multiprocess
import threading
import psutil

#format
import string
import json

#sqlite
import sqlite3

#args
import argparse

#maths
import numpy as np



#initialization: args + sqlite connection
def init():
    #parsing arguments
    parser = argparse.ArgumentParser(description='data processing from a sqlite openwsn db.')

    parser.add_argument('--db',
                        default="./openv_events.db",
                        help='location of the sqlite db', required=True)
    args = parser.parse_args()
    print("DB location:{0}".format(args.db))

    return(sqlite3.connect(args.db))


#returns the list of motes
def motes_get(con):
    motes = []
    cur = con.cursor()
    for row in cur.execute('SELECT DISTINCT moteid FROM sixtopStates'):
        motes.append(row[0])
    return(motes)



#returns the list of links (anycast = yes / no
def links_get(con):
    links = []
    cur = con.cursor()
    for row in cur.execute('SELECT DISTINCT moteid, neighbor, neighbor2, anycast, asn, slotOffset, channelOffset  FROM schedule WHERE type="TX" AND shared="0" AND event="ADD"'):
        
        #search for the correspond DEL asn
        cur2 = con.cursor()
        asn_end = None
        
  
            
        for row2 in cur2.execute('SELECT asn  FROM schedule WHERE type="TX" AND shared="0" AND event="DEL" AND moteid="{0}" AND neighbor="{1}" AND slotOffset="{2}" '.format(row[0], row[1], row[5])):
             
            #keep the smallest ASN larger than the start_asn
            if row2[0] > row[4] and ( asn_end is None or row2[0] < asn_end):
                asn_end = row2[0]

        links.append({'src':row[0], 'neigh':row[1], 'neigh2':row[2], 'anycast':row[3], 'slot':row[5], 'channel':row[6], 'start':row[4], 'end':asn_end })
    return(links)



#returns the list of cexample packets, and each corresponding l2 transmission
def packets_end2end(con):
    packets = []

    cur_cexample = con.cursor()
    for packet in cur_cexample.execute('SELECT DISTINCT moteid,seqnum,asn,buffer_pos FROM application WHERE component="CEXAMPLE"'):
        
        moteid      = packet[0]
        seqnum      = packet[1]
        asn_gen     = packet[2]
        buffer_pos  = packet[3]
        packets.append({'src': moteid, 'seqnum':seqnum, 'asn':asn_gen, 'buffer_pos':buffer_pos, 'packets':[] })
        
        
        #be carefull -> cexample allocates one packet, and UDP allocates another one (distinct)
        cur_udp = con.cursor()
        cur_udp.execute('SELECT buffer_pos FROM queue WHERE moteid="{0}" AND asn="{1}" AND event="ALLOCATE" AND buffer_pos!="{2}"'.format(moteid, asn_gen, buffer_pos))
        results = cur_udp.fetchall()
        if (len(results) != 1):
            print("Hum, we have too many (or zero) possible responses for this packet:")
            for row in results:
                print(row)
        else:
            buffer_pos = results[0][0]
         
         
        
        #for each hop, computes the duration in the queue
        asn_add = asn_gen
        cur_queue = con.cursor()
        cur_queue.execute('SELECT asn FROM queue WHERE moteid="{0}" AND asn>="{1}" AND event="DELETE" AND buffer_pos="{2}" ORDER BY asn ASC '.format(moteid, asn_add, buffer_pos))
        asn_del = cur_queue.fetchone()[0]
        print("{2}(seqnum{3}, pos{4}): {0} -> {1}".format(asn_add, asn_del, moteid, seqnum, buffer_pos))
        
        #get the corresponding list of TX
        cur_tx = con.cursor()
        for tx in cur_tx.execute('SELECT asn, slotOffset, channelOffset FROM pkt WHERE moteid="{0}" AND event="TX" AND type="DATA" AND buffer_pos="{1}" AND asn<="{2}" AND asn>="{3}" '.format(moteid, buffer_pos, asn_del, asn_add)):
            print("txdata: asn={0}".format(tx[0]))
        
            #and the coresponding list of RX
            cur_txack = con.cursor()
            for txack in cur_txack.execute('SELECT moteid, buffer_pos FROM pkt WHERE event="TX" AND type="ACK" AND asn="{0}"'.format(tx[0])):
                print("txack: {0}".format(txack))
                
                
            #and the coresponding list rcved acks
            cur_rxack = con.cursor()
            for rxack in cur_rxack.execute('SELECT moteid, buffer_pos FROM pkt WHERE event="RX" AND type="ACK" AND asn="{0}" '.format(tx[0])):
                print("rxack: {0}".format(rxack))
             
       
       
       
       
    return(packets)
        
        
        


#Initialization (DB)
con = init()

#extracts preliminary info
print("----- Motes -----")
motes = motes_get(con)
print(motes)

#anycast links
print("------ Links -----")
links = links_get(con)
for link in links:
    print(link)

#track tx for each app packet
print("------ Packets -----")
packets = packets_end2end(con)
for packet in packets:
    print(packet)





#end
con.close()
print("End of the computation")
sys.exit(0)



