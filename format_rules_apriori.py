#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 08:42:59 2018

@author: natalie
"""

#after you run the patterns and rules generation from pypf


#this output of the rules dataframe is an input here

#Bring in the frequency of each rule and sum of the normalized latency. Use these metrics to prioritize the rules we will implement. There is always a tradeoff withh implementing more rules, so we want to be efficient.


def format_rules_apriori(rules, orig_df,apps_server): 
    import pandas as pd
    import numpy as np

    rules.rename(columns={'item_A':'IP_A', 'item_B':'IP_B'}, inplace=True)
    rules['pairs']=list(zip(rules.IP_A, rules.IP_B))
    rules['pairs']=rules['pairs'].apply(sorted)
    
    rules['pairs2']=tuple(rules['pairs'])
    pairs_count=(orig_df.groupby('pairs2').agg({'Date':'count', 'norm_latency': 'mean', 'Duration': 'sum', 'Packets':'sum'}).reset_index())
    pairs_count.columns=['pairs','frequency', 'avg_norm_latency', 'total_duration', 'total_packets']
    pairs_count['norm_latency']=(pairs_count['total_duration']/pairs_count['total_packets'].sum())*100 #sum of all duration time divided by sum of all packets transfered for that pair
    
    rules_df=rules.merge(pairs_count, left_on='pairs2', right_on='pairs')
    rules_df=rules_df.drop('pairs_y', axis=1)
    rules_df=rules_df.rename(columns={'pairs_x':'pairs'})
    rules_df['latency_rank']=rules_df['frequency']*rules_df['norm_latency']
    
    rules_df=rules_df.sort_values(by='latency_rank', ascending=False)
    
    #1. Start by filling in the servers on the pairs until the server is full
    import math
    pairs_server=apps_server/2 #pairs of IP addresses that can fit on each server

#how many servers do we need for our rules, which are in pairs?
    servers_rule=math.ceil(len(rules_df)/pairs_server)
    servers_rule_list=list(range(0,servers_rule+1))
    servers_rule_list=np.repeat(servers_rule_list,pairs_server)

#remove the extra items
    servers_rule_list=servers_rule_list[0:len(rules_df)]
    
    # add a pair_ID column so we can keep track of how frequently IP addresses repeat in different pairs
    rules_df['pair_ID']=range(0, len(rules_df))
    
    #start by assigning the most important pair to a server
    rules_df['server_A']=None
    rules_df['server_B']=None
    rules_df.loc[rules_df['pair_ID']==0, 'server_A'] = 0
    rules_df.loc[rules_df['pair_ID']==0, 'server_B'] = 0
    
    #assign these servers to the pairs in our rules dataframe. Again this is stupid as we are not considering individual IPs that may repeat in different pairs. but it's a start
    rules_df['server']=servers_rule_list
    rules_df.index=range(0, len(rules_df))#need to reset index


    return(rules_df)
