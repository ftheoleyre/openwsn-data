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

#FIFO
from collections import deque




#Constants
NBCORES = 8
NBROWS= 200
DEBUG = False
DB_FILENAME = ""
    
   

    
class rxFound(Exception):
    """
    The final destinatio has been found
    """
    pass
    


#initialization: args + sqlite connection
def init():
    #parsing arguments
    parser = argparse.ArgumentParser(description='data processing from a sqlite openwsn db.')

    parser.add_argument('--db',
                        default="./openv_events.db",
                        help='location of the sqlite db')
    args = parser.parse_args()
    print("DB location:{0}".format(args.db))
    global DB_FILENAME
    DB_FILENAME = args.db


#multithreading is here safe because we NEVER modify the db, we just read it
def db_create_connection():
    return(sqlite3.connect(DB_FILENAME, check_same_thread=False))


#returns the list of motes
def motes_get(con):
    motes = []
    cur = con.cursor()
    for row in cur.execute('SELECT DISTINCT moteid FROM sixtopStates'):
        motes.append(row[0])
    return(motes)


#returns the list of dagroot ids
def dagroot_ids_get(con):
    dagroot_ids = []
    cur = con.cursor()
    for row in cur.execute('SELECT DISTINCT moteid FROM config WHERE rpl_dagroot="1"'):
        dagroot_ids.append(row[0])
    return(dagroot_ids)



#returns the largest ASN in the experiment
def asn_end_get(con):
    cur = con.cursor()
    for row in cur.execute('SELECT MAX(asn) as max FROM queue'):
        return(row[0])
    return(0)




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


##returns the list of receivers for this l2 transmission
def cex_l2receivers_get(con, l2src, asn):

    receivers = []
    cur_rx = con.cursor()
    for rx in cur_rx.execute('SELECT moteid, buffer_pos, crc, rssi, priority FROM pkt WHERE event="RX" AND type="DATA" AND asn="{0}" AND l2src="{1}"'.format(asn, l2src)):
        if DEBUG:
            print("   rx(anycast, {1}): {0}".format(rx, l2src))
        moteid      = rx[0]
        buffer_pos  = rx[1]
        crc         = rx[2]
        rssi        = rx[3]
        priority    = rx[4]
        
        #if the packet has been received correctly, track the corresponding ack tx
        ack_txed = 0
        if (crc == 1):
            cur_acktx = con.cursor()
            cur_acktx.execute('SELECT moteid FROM pkt WHERE event="TX" AND type="ACK" AND asn="{0}" AND l2src="{1}"'.format(asn, rx[0]))
            results = cur_acktx.fetchall()
            
            
            
            #an ack has been txed -> it will try to forward the packet
            if (len(results) == 0):     # probably second receiver in anycast
                ack_txed = 0
            
            elif (len(results) == 1):       #an ack has been txed
                ack_txed = 1
                 
            elif DEBUG:
                print("Hum, several acks from the same moteid? sounds strange....")
                print(results)
                print('SELECT moteid FROM pkt WHERE event="TX" AND type="ACK" AND asn="{0}" AND l2src="{1}"'.format(asn, rx[0]))
        else:
            print("BAD CRC, not POPPED")
         
        #insert this receiver for this hop
        receivers.append({'moteid':moteid, 'crc':crc, 'rssi':rssi, 'buffer_pos':buffer_pos, 'priority':priority, 'ack_txed':ack_txed})
        
    return(receivers)
   


#list all the l2 transmissions for a given mote (packet in the queue of a mote)
#be careful: a mote may receive the same cex_packet several times, each will constitute a different "hop"
def cex_l2transmissions_for_hop(con, l2tx_list, processingQueue, elem):
    
    #for each TX *and* RETX in this two-ASN interval
    cur_tx = con.cursor()
    for tx in cur_tx.execute('SELECT asn, slotOffset, channelOffset, l2dest FROM pkt WHERE moteid="{0}" AND event="TX" AND type="DATA" AND buffer_pos="{1}" AND asn<="{2}" AND asn>="{3}" '.format(elem['l2src'], elem['buffer_pos'], elem['asn_del'], elem['asn_add'])):
        asn             = tx[0]
        slotOffset      = tx[1]
        channelOffset   = tx[2]
        l2dest          = tx[3]
        #print("txdata: src={1} & asn={0}".format(asn, elem['l2src']))

        #an ack has been correctly received? (crc ok)
        cur_rxack = con.cursor()
        ack_rcvd = 0
        cur_rxack.execute('SELECT moteid, buffer_pos FROM pkt WHERE event="RX" AND type="ACK" AND asn="{0}" AND crc="1" '.format(asn))
        results = cur_rxack.fetchall()
        if (len(results) > 0):
            ack_rcvd = 1

        #list the l2 receivers for this l2 transmission
        receivers = cex_l2receivers_get(con, elem['l2src'], asn)
        
        #add the receivers to the processing list (if it sent an ack, it means it will forward the packet)
        for rcvr in receivers:
            if rcvr['ack_txed'] == 1:
                if (DEBUG):
                    print("TO PROCESS: id={0}, asn={1}, buffer_pos={2}".format(rcvr['moteid'], asn, rcvr['buffer_pos']))
                processingQueue.append({'l2src':rcvr['moteid'], 'buffer_pos':rcvr['buffer_pos'], 'asn_add':asn})
                
           
        #we have listed everything for this hop
        l2tx_list.append({'asn':tx[0], 'l2src':elem['l2src'], 'buffer_pos':elem['buffer_pos'], 'slotOffset':slotOffset, 'channelOffset':channelOffset, 'l2dest':l2dest, 'ack_rcvd':ack_rcvd,  'receivers':receivers })
        
        if DEBUG:
            print("")
            print("")
            


#list all the l2 transmissions for a given cexample packet
def cex_l2transmissions_for_cex_packet(con, moteid, buffer_pos, asn_gen):
    l2tx_list = []

    #list of motes (to be processed) which recived (and will forward) the packet
    processingQueue = deque()
    processingQueue.append({'l2src':moteid, 'buffer_pos':buffer_pos, 'asn_add':asn_gen})
    
    cur_queue = con.cursor()

    # until the processing queue is empty (each hop that receives this packet is inserted in the FIFO)
    while (len(processingQueue) > 0):
        elem = processingQueue.popleft()
         
        if DEBUG:
            print("")
            print("POP: id={0}, asn={1}".format(elem['l2src'], elem['asn_add']))


        #ASN of deletion
        #print('SELECT asn FROM queue WHERE moteid="{0}" AND asn>="{1}" AND event="DELETE" AND buffer_pos="{2}" ORDER BY asn ASC '.format(moteid, asn_add, buffer_pos))
        cur_queue.execute('SELECT asn FROM queue WHERE moteid="{0}" AND asn>="{1}" AND event="DELETE" AND buffer_pos="{2}" ORDER BY asn ASC '.format(elem['l2src'], elem['asn_add'], elem['buffer_pos']))

        #no result -> the packet has not been txed / dropped -> discard it
        results = cur_queue.fetchall()
        if (len(results) == 0):
            return None
        elem['asn_del'] = results[0][0]
            
        
        if (DEBUG):
            print("Deletion : {0}".format(asn_del))
         
         
        #handle this hop
        cex_l2transmissions_for_hop(con, l2tx_list, processingQueue, elem)
      

        #that's the end: all the transmission have been handled for this cexample packet
        if DEBUG:
            print("    {0}".format(cex_packet_l2tx))
         
        if DEBUG:
            print("")
            print("-----------")
    return(l2tx_list)
 
 
 
#separate function for each cexample packet so that it can multithreaded
def cex_packet_handle(packet):
            
    moteid      = packet[0]
    seqnum      = packet[1]
    asn_gen     = packet[2]
    buffer_pos  = packet[3]
    cex_packet = ({'cex_src': moteid, 'seqnum':seqnum, 'asn':asn_gen})
    
    global DEBUG
    if moteid == 'XXXXXXX':
        DEBUG = True
    else:
        DEBUG = False
    
    if DEBUG:
        print("")
        print("-------   {0} seqnum={1} --------".format(moteid, seqnum))

    #be carefull -> cexample allocates one packet, and UDP allocates another one (distinct)
    #thus, selects the UDP packet generated by the same moteid, with the same ASN (we would be unlucky if we have collisions -- i.e., several CoAP/UDP applications in parallel)
    cur_udp = con.cursor()
    cur_udp.execute('SELECT buffer_pos FROM application WHERE moteid="{0}" AND asn="{1}" AND component="SOCK_TO_UDP"'.format(moteid, asn_gen))
    results = cur_udp.fetchall()
    if (len(results) != 1):
        print("Hum, we have too many (or zero) possible responses (={3}) for this packet (id {0}, asn={1}, pos={2}):".format(moteid, asn_gen, buffer_pos, len(results)))
        
        print('SELECT buffer_pos FROM application WHERE moteid="{0}" AND asn="{1}" AND component="SOCK_TO_UDP"'.format(moteid, asn_gen))
        
        for row in results:
            print(row)
    else:
        buffer_pos = results[0][0]

    # will not explore all the transmissions for this cexample packet
    cex_packet['l2_transmissions'] = cex_l2transmissions_for_cex_packet(con, moteid, buffer_pos, asn_gen)
    if cex_packet['l2_transmissions'] is not None:
        return(cex_packet)

                    
        
#returns the list of cexample packets, and each corresponding l2 transmission
def cex_packets_end2end(con):
    cex_packets_stats = []
    
    #handles each cexample packet
    cur_cexample = con.cursor()
    for packet in cur_cexample.execute('SELECT DISTINCT moteid,seqnum,asn,buffer_pos FROM application WHERE component="CEXAMPLE"'):
   
        result = cex_packet_handle(packet)
        if result is not None:
            cex_packets_stats.append(result)
   
    return(cex_packets_stats)
          
    
    
    
#main (multithreading safe)
if __name__ == "__main__":

    #parameters, etc.
    init()
    con = db_create_connection()

    #extracts preliminary info
    print("")
    print("----- Motes -----")
    motes = motes_get(con)
    print(motes)
    dagroot_ids = dagroot_ids_get(con)
    print("dagroots = {0}".format(dagroot_ids))
    asn_end = asn_end_get(con)
    print("ASN max = {0}".format(asn_end))

    #anycast/unicast links
    print("")
    print("------ Links / Cells -----")
    links = links_get(con)
    for link in links:
        print(link)

    #track tx for each app packet
    print("")
    print("------ Cexample Packets -----")
    print("")
    print("")
    cex_packets = cex_packets_end2end(con)


    if False:
        for cex_packet in cex_packets:
            print(cex_packet)
        
            print("src={0}, seqnum={1}".format(cex_packet['cex_src'], cex_packet['seqnum']))
            for elem in cex_packet['l2_transmissions']:
                print("        {0}".format(cex_packet['l2_transmissions']))
            print("------")


    #track tx for each app packet
    print("")
    print("------ Statistics -----")

    #PDR per source
    stats = {}
    for cex_packet in cex_packets:
        
        #don't handle the packets that have been generated too late (may not be delivered to the sink)
       # if cex_packet['asn'] >= asn_end - 500 :
       #     break

        #creates the key for this src device if it doesn't exist yet
        if cex_packet['cex_src'] in stats:
            stats[cex_packet['cex_src']]['nb_gen'] = stats[cex_packet['cex_src']]['nb_gen'] + 1
        else:
            stats[cex_packet['cex_src']] = {}
            stats[cex_packet['cex_src']]['nb_gen'] = 1
            stats[cex_packet['cex_src']]['nb_rcvd'] = 0
            stats[cex_packet['cex_src']]['delay'] = 0
            stats[cex_packet['cex_src']]['nb_l2tx'] = 0

        
        try:
            stats[cex_packet['cex_src']]['nb_l2tx'] += len(cex_packet['l2_transmissions'])
            for l2tx in cex_packet['l2_transmissions']:
                for rcvr in l2tx['receivers']:
                    if rcvr['moteid'] in dagroot_ids :
                        stats[cex_packet['cex_src']]['nb_rcvd'] += 1 #stats[cex_packet['cex_src']]['nb_rcvd'] +1
                        stats[cex_packet['cex_src']]['delay'] += l2tx['asn'] - cex_packet['asn']
                        raise(rxFound)
        #we found one of the dagroot ids -> pass to the next cex packet
        except rxFound as evt:
            pass
      
    print("Per cexample packet")
    for value in stats:
        print("{0}: PDR={1}% / delay={2}slots / nbl2tx_eff={3} / nbl2tx_raw={4} / stats={5}".format(
        value,
        100 * stats[value]['nb_rcvd'] / stats[value]['nb_gen'] ,
        stats[value]['delay'] / stats[value]['nb_rcvd'],
        stats[value]['nb_l2tx'] / stats[value]['nb_rcvd'],
        stats[value]['nb_l2tx'] / stats[value]['nb_gen'],
        stats[value]
        ))

    print("")
    print("")



    #debug for one source
    if DEBUG:
        for cex_packet in cex_packets:
            if cex_packet['cex_src'] == '054332ff03dc9769' :
                print("")
                print("seqnum {0}".format(cex_packet['seqnum']))
                print("")

                for l2tx in cex_packet['l2_transmissions']:
                    print(l2tx)

                print("")
                print("")

    print("------ Configuration -----")
    
    print("dagroots: {0}".format(dagroot_ids))

    #end
    print("End of the computation")
    con.close()
    sys.exit(0)



