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
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns






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

    
    
    
#main (multithreading safe)
if __name__ == "__main__":

    #parameters
    directory = init()

    #two dictionaries (anycast or not)
    data = {}
    allvalues = {}
    allvalues['nbmotes'] = []
    allvalues['sixtop_anycast'] = []
    allvalues['pdr'] = []
    allvalues['delay_ts'] = []
    allvalues['delay_ms'] = []
    allvalues['nb_l2tx_raw'] = []
    allvalues['nb_l2tx_norm'] = []
           
  
    for experiment in os.listdir(directory):
        json_filename = os.path.join(directory, experiment, "stats.json")
        
        if os.path.isfile(json_filename) is True:
            print(json_filename)
            with open(json_filename) as json_file:
                datafile = json.load(json_file)
   
            #put the data in a global data variable
            nbmotes = str(datafile['configuration']['nbmotes'])
            anycast = str(datafile['configuration']['sixtop_anycast'])
            
            #creates tjhe keys if they don't exist in the dict
            if nbmotes not in data:
                data[nbmotes] = {}
            if anycast not in data[nbmotes]:
                data[nbmotes][anycast] = []
            
            #novel data
            data[nbmotes][anycast].append(datafile)
   
            print(datafile['configuration'])
          
          
            #track tx for each app packet
            print("")
            print("------ Statistics -- {0} -----".format(experiment))

            #PDR per source
            stats = {}
            for cex_packet in datafile['cex_packets']:
                
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
                            if rcvr['moteid'] in datafile['dagroot_ids'] :
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
                
                
                #everything in a big array (one line per flow)
                allvalues['nbmotes'].append(datafile['configuration']['nbmotes'])
                allvalues['sixtop_anycast'].append(datafile['configuration']['sixtop_anycast'])
                allvalues['pdr'].append(100 * stats[value]['nb_rcvd'] / stats[value]['nb_gen'])
                allvalues['delay_ts'].append(stats[value]['delay'] / stats[value]['nb_rcvd'])
                allvalues['delay_ms'].append(25 * stats[value]['delay'] / stats[value]['nb_rcvd'])
                allvalues['nb_l2tx_raw'].append(stats[value]['nb_l2tx'] / stats[value]['nb_gen'])
                allvalues['nb_l2tx_norm'].append(stats[value]['nb_l2tx'] / stats[value]['nb_rcvd'])
                

            print("")


            #debug for one source
            if False:
                for cex_packet in cex_packets:
                    if cex_packet['cex_src'] == '054332ff03dc9769' :
                        print("")
                        print("seqnum {0}".format(cex_packet['seqnum']))
                        print("")

                        for l2tx in cex_packet['l2_transmissions']:
                            print(l2tx)

                        print("")
                        print("")


            print("-- Configuration")
            
            print("dagroots: {0}".format(datafile['dagroot_ids']))


    #Panda values (separating anycast and unicast)
    data_pd = pd.DataFrame.from_dict(allvalues)
    print(data_pd)
    
    #common sns config
    sns.set_theme(style="ticks")

    #PDR
    sns.violinplot(x='nbmotes', y='pdr', hue='sixtop_anycast',cut=0, data=data_pd)
    plt.ylabel("Packet Delivery Ratio", size=12)
    plt.xlabel("Number of motes", size=12)
    sns.despine(trim=True, left=True)
    plt.savefig("figs/pdr.pdf")
    
    #PDR
    plt.clf()
    sns.violinplot(x='nbmotes', y='delay_ms', hue='sixtop_anycast',data=data_pd)
    plt.ylabel("Delay (in ms)", size=12)
    plt.xlabel("Number of motes", size=12)
    plt.savefig("figs/delay.pdf")
    
    #Efficiency
    plt.clf()
    sns.violinplot(x='nbmotes', y='nb_l2tx_raw', hue='sixtop_anycast',data=data_pd)
    plt.ylabel("Number of transmissions per message", size=12)
    plt.xlabel("Number of motes", size=12)
    plt.savefig("figs/nb_l2tx.pdf")

   

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



