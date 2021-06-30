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


#plot
import pandas as pd
import seaborn as sns
from IPython.display import display



PARAM_MIN_TX = 70       #discard all the stats where less than X packets have been generated
PARAM_MIN_ACKTX = 40    # disard flows with not enough ack txed
PARAM_ASN_MARGIN_START = 1000  # discard the last X timeslots (25ms)
PARAM_ASN_MARGIN_END = 500     # discard the first X timeslots (25ms)


class rxFound(Exception):
    """
    The final destinatio has been found
    """
    pass
    
    

#initialization: args + sqlite connection
def init():
    #parsing arguments
    parser = argparse.ArgumentParser(description='plot graphs with the data processed in a json file.')

    parser.add_argument('--dir',
                        default="./results",
                        help='directory to parse (one directory per experiment)')
    args = parser.parse_args()
    print("DIR:{0}".format(args.dir))
    return(args.dir)

    
#extracts and classifies cexample info
def cexample_compute(datafile, flowStats):
        
    #track tx for each app packet
    print("")
    print("------ Statistics -- {0} -----".format(experiment))

    #PDR per source
    stats = {}
    for cex_packet in datafile['cex_packets']:
        
        #don't handle the packets that have been generated too late or too early
        if cex_packet['asn'] < PARAM_ASN_MARGIN_START or cex_packet['asn'] > datafile['asn_end'] - PARAM_ASN_MARGIN_END :
            break

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
                    if rcvr['moteid'] in datafile['dagroot_ids'] :
                        stats[cex_packet['cex_src']]['nb_rcvd'] += 1 #stats[cex_packet['cex_src']]['nb_rcvd'] +1
                        stats[cex_packet['cex_src']]['delay'] += l2tx['asn'] - cex_packet['asn']
                        raise(rxFound)
        #we found one of the dagroot ids -> pass to the next cex packet
        except rxFound as evt:
            pass
    
    print("Per cexample packet")
    for value in stats:
       
        #if the stat is significant (synchronized node)
        if (stats[value]['nb_gen'] > PARAM_MIN_TX):
    
                
    
            if (stats[value]['nb_rcvd'] > 0):
                print("{0}: PDR={1}% / delay={2}slots / nbl2tx_eff={3} / nbl2tx_raw={4} / stats={5}".format(
                value,
                100 * stats[value]['nb_rcvd'] / stats[value]['nb_gen'] ,
                stats[value]['delay'] / stats[value]['nb_rcvd'],
                stats[value]['nb_l2tx'] / stats[value]['nb_rcvd'],
                stats[value]['nb_l2tx'] / stats[value]['nb_gen'],
                stats[value]
                ))
            else:
                print("{0}: PDR={1}% / delay={2}slots / nbl2tx_eff={3} / nbl2tx_raw={4} / stats={5}".format(
                value,
                100 * stats[value]['nb_rcvd'] / stats[value]['nb_gen'] ,
                0,
                0,
                stats[value]['nb_l2tx'] / stats[value]['nb_gen'],
                stats[value]
                ))
                
            
            #everything in a big array (one line per flow)
            flowStats['cex_src'].append(value)
            flowStats['nbmotes'].append(datafile['configuration']['nbmotes'])
            flowStats['cexample_period'].append(datafile['configuration']['cexample_period'])
            flowStats['sixtop_anycast'].append(datafile['configuration']['sixtop_anycast'])
            flowStats['pdr'].append(100 * stats[value]['nb_rcvd'] / stats[value]['nb_gen'])
            flowStats['nb_l2tx_raw'].append(stats[value]['nb_l2tx'] / stats[value]['nb_gen'])
            if (stats[value]['nb_rcvd'] > 0):
                flowStats['delay_ts'].append(stats[value]['delay'] / stats[value]['nb_rcvd'])
                flowStats['delay_ms'].append(25 * stats[value]['delay'] / stats[value]['nb_rcvd'])
                flowStats['nb_l2tx_norm'].append(stats[value]['nb_l2tx'] / stats[value]['nb_rcvd'])
            else:
                flowStats['delay_ts'].append(0)
                flowStats['delay_ms'].append(0)
                flowStats['nb_l2tx_norm'].append(0)

            
                    
                    
#plot the figures for cexample stats
def cexample_plot(flowStats):
    
    #Panda values (separating anycast and unicast)
    print("-- Cexample flows")
    flowStats_pd = pd.DataFrame.from_dict(flowStats)
    print(flowStats_pd)
    
    #common sns config
    sns.set_theme(style="ticks")

    #PDR
    plot = sns.violinplot(x='nbmotes', y='pdr', hue='sixtop_anycast',cut=0, data=flowStats_pd)
    plot.set_xlabel("Number of motes")
    plot.set_ylabel("Packet Delivery Ratio")
    plot.figure.savefig("figs/cexample_pdr.pdf")
    plot.set_xticks(np.arange(50, 100, 10))
    plot.figure.clf()
    
    #PDR
    plot = sns.violinplot(x='nbmotes', y='delay_ms', hue='sixtop_anycast',data=flowStats_pd)
    plot.set_xlabel("Number of motes")
    plot.set_ylabel("Delay (in ms)")
    plot.figure.savefig("figs/cexample_delay.pdf")
    plot.figure.clf()
        
    #Efficiency
    plot = sns.violinplot(x='nbmotes', y='nb_l2tx_raw', hue='sixtop_anycast',data=flowStats_pd)
    plot.set_xlabel("Number of motes")
    plot.set_ylabel("Number of transmissions per message")
    plot.figure.savefig("figs/cexample_nb_l2tx.pdf")
    plot.figure.clf()
   
    print("")
    print("")
    



#raw layer 2 transmissions
def l2tx_compute(datafile, l2txStats):

    # ------- Raw l2 tx -------
    print("-- l2 tx")

    #Panda values (separating anycast and unicast)
    l2tx_pdr_raw_pd = pd.DataFrame.from_dict(datafile['l2tx'])

    #discard shared and auto cells
    l2tx_pdr_raw_pd = l2tx_pdr_raw_pd[
        (l2tx_pdr_raw_pd['shared'] == 0) &
        (l2tx_pdr_raw_pd['autoCell'] == 0) &
        (l2tx_pdr_raw_pd['asn'] > PARAM_ASN_MARGIN_START) &
        (l2tx_pdr_raw_pd['asn'] < datafile['asn_end'] - PARAM_ASN_MARGIN_END)
    ]
    
    #group by TX (to count the  of txed packets)
    l2tx_groupbycelltx_pd = l2tx_pdr_raw_pd.groupby(["moteid_tx","slotOffset", "channelOffset"]).agg({"asn":"count"}).reset_index().sort_values(by=['slotOffset'])
    
    #group by RX (to count the of rcvd packets)
    l2tx_groupbycellrx_pd = l2tx_pdr_raw_pd.groupby(["moteid_tx","slotOffset", "channelOffset", "moteid_rx"], dropna=True).agg({"asn":"count","priority_rx":"mean", "crc_data":"sum", "ack_tx":"sum", "crc_ack":"sum"}).reset_index().sort_values(by=['slotOffset'])
    

    print("------")
    display(l2tx_groupbycelltx_pd)
    print("------")
    print("------")
    display(l2tx_groupbycellrx_pd)
    print("------")
    


    #notna = pd.notna(l2tx_groupbycellrx_pd["moteid_rx"])
    for index in l2tx_groupbycellrx_pd.index:
        
        #retrieves the number of txed packets in this cell
        numTx_pd = l2tx_groupbycelltx_pd[(l2tx_groupbycelltx_pd['moteid_tx'] == l2tx_groupbycellrx_pd['moteid_tx'][index]) ].reset_index()
        numTx_pd = numTx_pd[(numTx_pd['slotOffset'] == l2tx_groupbycellrx_pd['slotOffset'][index]) ].reset_index()
        numTx = numTx_pd['asn'].iloc[0]
                 
          
        #everything in a big array (one line per flow)
        if (numTx > PARAM_MIN_TX) and (l2tx_groupbycellrx_pd['ack_tx'][index] > PARAM_MIN_ACKTX) :
            l2txStats['nbmotes'].append(datafile['configuration']['nbmotes'])
            l2txStats['cexample_period'].append(datafile['configuration']['cexample_period'])
            l2txStats['sixtop_anycast'].append(datafile['configuration']['sixtop_anycast'])
            l2txStats['moteid_tx'].append(l2tx_groupbycellrx_pd['moteid_tx'][index])
            l2txStats['moteid_rx'].append(l2tx_groupbycellrx_pd['moteid_rx'][index])
            l2txStats['slotOffset'].append(l2tx_groupbycellrx_pd['slotOffset'][index])
            l2txStats['priority_rx'].append(l2tx_groupbycellrx_pd['priority_rx'][index])
            l2txStats['numDataTx'].append(numTx)
            l2txStats['numDataRx'].append(l2tx_groupbycellrx_pd['crc_data'][index])
            l2txStats['PDRData'].append(l2tx_groupbycellrx_pd['crc_data'][index] / numTx)
            l2txStats['numAckTx'].append(l2tx_groupbycellrx_pd['ack_tx'][index])
            l2txStats['numAckRx'].append(l2tx_groupbycellrx_pd['crc_ack'][index])
            l2txStats['PDRAck'].append(l2tx_groupbycellrx_pd['crc_ack'][index] / l2tx_groupbycellrx_pd['ack_tx'][index])
      
        
    print("--------")
 
                    
#plot the figures for cexample stats
def l2tx_plot(l2txStats):
    
    #Panda values (separating anycast and unicast)
    print("-- l2txStats statistics")
    l2txStats_pd = pd.DataFrame.from_dict(l2txStats)
    print(l2txStats_pd)
    
    #common sns config
    sns.set_theme(style="ticks")

    #PDR
    plot = sns.scatterplot(x='PDRData', y='PDRAck', data=l2txStats_pd)
    plot.set_xlabel("Packet Delivery Ratio (data)")
    plot.set_ylabel("Packet Delivery Ratio (ack)")
    plot.figure.savefig("figs/l2tx_pdr_bidirect.pdf")
    plot.figure.clf()
    
  
   
    print("")
    print("")
    





#main (multithreading safe)
if __name__ == "__main__":

    #parameters
    directory = init()

    #stats for cexample flows (dictionary to be used later by pandas)
    flowStats = {}
    flowStats['cex_src'] = []
    flowStats['nbmotes'] = []
    flowStats['cexample_period'] = []
    flowStats['sixtop_anycast'] = []
    flowStats['pdr'] = []
    flowStats['delay_ts'] = []
    flowStats['delay_ms'] = []
    flowStats['nb_l2tx_raw'] = []
    flowStats['nb_l2tx_norm'] = []
           
    #stats for l2 frame transmsisions
    l2txStats = {}
    l2txStats['nbmotes'] = []
    l2txStats['cexample_period'] = []
    l2txStats['sixtop_anycast'] = []
    l2txStats['moteid_tx'] = []
    l2txStats['moteid_rx'] = []
    l2txStats['slotOffset'] = []
    l2txStats['priority_rx'] = []
    l2txStats['numDataTx'] = []
    l2txStats['numDataRx'] = []
    l2txStats['PDRData'] = []
    l2txStats['numAckTx'] = []
    l2txStats['numAckRx'] = []
    l2txStats['PDRAck'] = []

  
    for experiment in os.listdir(directory):
        json_filename = os.path.join(directory, experiment, "cexample.json")
        
        if os.path.isfile(json_filename) is True:
            print(json_filename)
            with open(json_filename) as json_file:
                datafile = json.load(json_file)
           
            #organizes the stats for cexample
            cexample_compute(datafile, flowStats)

            #compute the stats for l2tx
            l2tx_compute(datafile, l2txStats)

   
    #plot the figures for cexample
    cexample_plot(flowStats)
    l2tx_plot(l2txStats)
    
    
   
   
   
   
   
   
   
   
   
 
    #end
    print("End of the computation")
    if False:
        for nbmotes in data:
            for anycast in data[nbmotes]:
                print("{0} motes, anycast={1}".format(nbmotes, anycast))
            
                for experiment in data[nbmotes][anycast]:
                    print("   {0}".format(experiment['configuration']))
                
                print("")
                
            
            
            
    sys.exit(0)



