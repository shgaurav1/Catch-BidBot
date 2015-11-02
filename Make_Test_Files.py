#!/usr/bin/env python 
# Reads the raw input file and for each bidder id creates a new file with the bid details. Also generates the train_version files to be used for classifiers
# File by -- Gaurav Shrivastava, BITS-Pilani

import os, sys, pickle
import numpy as np
EXTRACTED = 1 # data extracted from the train/test files 
TIMEdigits = 6 # (also try 6! and 8!) if first  'TIMEdigits' are same of the prev and curr timestamp, increase the count by 1. Should catch bots as more frequent transactions
VERSION = '2' # The version of train file 

def extract_dict(bids_file):# using dictionary 
    bidder_dict = {} # creating an empty dictionary
    line_num = 0
    for bids_line in file(bids_file):
        line_num += 1
        if line_num % 1000000 == 0:
            print line_num
        if line_num == 1:
            continue
        else:  
            bids_line = bids_line.strip()
            raw_bids_line = bids_line
            bids_line = bids_line.split(',')
            try:
                bidder_dict[bids_line[1]].append(raw_bids_line)
            except KeyError:
                bidder_dict[bids_line[1]] = [raw_bids_line]
#            print bidder_dict
    # save the dictionary using pickle
    with open('bidder_dict.pickle', 'w') as fid:
        pickle.dump(bidder_dict, fid)
    return bidder_dict




def build_features_dict(bidder_file, newpath): # build features from the existing dictionary for test data : bidder_file == test/train file
    # loading the dictionary 
    print 'Loading bidder dictionary'
    with open('bidder_dict.pickle') as fid:
        bidder_dict = pickle.load(fid)
    print 'Loading compare dictionaries'
    with open('compare_dict_3_6_8.pickle') as fid1:
        country_compare, merchandise_compare, url_compare = pickle.load(fid1)
    with open('compare_dict_2_4_7.pickle') as fid2:
        auction_compare, device_compare, ip_compare = pickle.load(fid2)
    with open('auction_winner_dict.pickle') as fid3:
        auction_winner_dict = pickle.load(fid3) 
    print 'dictionaries loaded'
    inv_dict_auction_winner = {}
    for k, v in auction_winner_dict.items():
        inv_dict_auction_winner[v] = inv_dict_auction_winner.get(v, [])
        inv_dict_auction_winner[v].append(k)

    # file to store the processes features of test file
    f1 = open(newpath+'_v'+VERSION+'.csv', 'w')
    # generating the testing dataset
    line_num = 0
    NOT_FOUND = 0
    for bidder_line in file(bidder_file):
        line_num += 1
        print line_num
        if(line_num == 1):
            continue
        else:
            bidder_line = bidder_line.strip()
            bidder_line = bidder_line.split(',')
            # store each bidder details in an array:data,  which is extracted from the dictionary   
            # empty array check
            bidder_id = bidder_line[0]
            if bidder_id in bidder_dict:
                label = bidder_line[0] #<outcome>
                print bidder_line[0] 
                data = bidder_dict[bidder_line[0]] # ['some_crap1', 'some_crap2', ...]
                # condition if only 1 line of data is present
                if len(data)==1:
                    data = data[0].split(',')
                    country = data[6]
                    if data[6]=='':
                        country = 'xx'
                    if country in country_compare:
                        country_prob_score = float(country_compare[country][1])/float(country_compare[country][0])
                    else:
                        country_prob_score = 'NA'                    
                    
                    url_type = data[8]
                    if url_type in url_compare:
                        if float(url_compare[url_type][0])!=0:
                            url_prob_score = float(url_compare[url_type][1])/float(url_compare[url_type][0])
                        else:
                            url_prob_score = float(url_compare[url_type][1])
                    else:
                        url_prob_score = 'NA'
                    
                    
                    auction_type = data[2]
                    if auction_type in auction_compare:
                        if float(auction_compare[auction_type][0]) != 0:
                            auction_prob_score = float(auction_compare[auction_type][1])/float(auction_compare[auction_type][0])
                        else:
                            auction_prob_score = float(auction_compare[auction_type][1])
                    else:
                        auction_prob_score = 'NA'
                    
                    device_type = data[4]
                    if device_type in device_compare:
                        if float(device_compare[device_type][0]) != 0:
                            device_prob_score = float(device_compare[device_type][1])/float(device_compare[device_type][0])
                        else:
                            device_prob_score = float(device_compare[device_type][1])
                    else:
                        device_prob_score = 'NA'                
    
                    ip_type = data[7]
                    if ip_type in ip_compare:
                        if float(ip_compare[ip_type][0]) != 0:
                            ip_prob_score = float(ip_compare[ip_type][1])/float(ip_compare[ip_type][0])
                        else:
                            ip_prob_score = float(ip_compare[ip_type][1])
                    else:
                        ip_prob_score = 'NA'
                    
                    bidnum = data[0]
                    mean_bidnum = float(bidnum)
                    stddev_bidnum = 0

                    time = data[5]
                    mean_time = float(time)
                    stddev_time = 0                     
        
                    bidding_speed = 0   
                    if bidder_id in inv_dict_auction_winner:
                        num_auctions_won = len(inv_dict_auction_winner[bidder_id])
                    else:
                        num_auctions_won = 0 

                    f1.write('%s, 1, 1, %s, 1, 0, 1, 1, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n' %(label, float(merchandise_compare[data[3]][1])/float(merchandise_compare[data[3]][0]), country_prob_score, url_prob_score, auction_prob_score, device_prob_score, ip_prob_score, mean_bidnum, stddev_bidnum, mean_time, stddev_time, bidding_speed, num_auctions_won))  
                else: 
                    temp_data = []
                    for i in range(0, len(data)):
                        dlist = data[i].split(',')
                        temp_data.append(dlist)
                    data = np.transpose(temp_data)
                    num_bids = len(data[0]) # <bid_id>
                    different_auctions = len(set(data[2]))# <auction>
                
                    merchandise_type = data[3][0]
                    merchandise_prob_score = float(merchandise_compare[merchandise_type][1])/float(merchandise_compare[merchandise_type][0])
#                    different_merchandise = len(set(data[3]))# <merchandise>
                    different_devices = len(set(data[4]))# <device>
                    for i in range(0, len(data[5])): # <time>
                        if i==0:
                            prev = data[5][i]
                            curr = data[5][i]
                            bidding_frequency = 0 # <time> if the 1st 6 digits of current timestamp matches with the previous one, increase by 1 
                        else:
                            prev = curr
                            curr = data[5][i]
                            if curr[0:TIMEdigits]==prev[0:TIMEdigits]:
                                bidding_frequency += 1

                    different_countries = len(set(data[6])) #<countries>
                    countries_prob_score = 0
                    for country in set(data[6]):
                        if country in country_compare:
                            if country == '':
                                country = 'xx'
                            if float(country_compare[country][0])!=0:
                                countries_prob_score +=  float(country_compare[country][1])/float(country_compare[country][0]) 
                            else:
                                countries_prob_score += float(country_compare[country][1])
                        else:
                            print 'countries not found ', country

                    different_ip = len(set(data[7])) #<ip>
                    different_url = len(set(data[8])) #<url>

                    count_set = 0
                    len_set = len(set(data[8]))
                    
                    url_prob_score = 0
                    for url_type in set(data[8]):
                        if url_type in url_compare:
                            if float(url_compare[url_type][0])!=0:
                                url_prob_score = float(url_compare[url_type][1])/float(url_compare[url_type][0])
                            else:
                                url_prob_score = float(url_compare[url_type][1])
                        else:
                            url_prob_score = 'NA'
                            
                    auction_prob_score = 0
                    for auction_type in set(data[2]):
                        if auction_type in auction_compare:
                            if float(auction_compare[auction_type][0])!=0:
                                auction_prob_score += float(auction_compare[auction_type][1])/float(auction_compare[auction_type][0])
                            else:
                                auction_prob_score += float(auction_compare[auction_type][1])
                        else:
                            auction_prob_score = 'NA'
                            break
                
                    device_prob_score = 0
                    for device_type in set(data[4]):
                        if device_type in device_compare:
                            if float(device_compare[device_type][0])!=0:
                                device_prob_score += float(device_compare[device_type][1])/float(device_compare[device_type][0])
                            else:
                                device_prob_score += float(device_compare[device_type][1])
                        else:
                            device_prob_score = 'NA'
                            break

                    ip_prob_score = 0
                    for ip_type in set(data[7]):
                        if ip_type in ip_compare:
                            if float(ip_compare[ip_type][0])!=0:
                                ip_prob_score = float(ip_compare[ip_type][1])/float(ip_compare[ip_type][0])
                            else:
                                ip_prob_score = float(ip_compare[ip_type][1])
                        else:
                            ip_prob_score = 'NA'

                    # mean and std dev of bidding number       
                    bidnum = np.array(data[0], dtype=float)
                    mean_bidnum = bidnum.mean()
                    stddev_bidnum = bidnum.std()
                    
                    # mean and std dev of time
                    time = np.array(data[5], dtype=float)
                    mean_time = time.mean()
                    stddev_time = time.std()
                    
                    bidding_speed = float(bidding_frequency)/float(num_bids)
                    if bidder_id in inv_dict_auction_winner:
                        num_auctions_won = len(inv_dict_auction_winner[bidder_id])
                    else:
                        num_auctions_won = 0
                    # writing the features in an  outfile
                    f1.write('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n'%(label, num_bids, different_auctions, merchandise_prob_score, different_devices, bidding_frequency, different_countries, different_ip, different_url, countries_prob_score, url_prob_score, auction_prob_score, device_prob_score, ip_prob_score, mean_bidnum, stddev_bidnum, mean_time, stddev_time, bidding_speed, num_auctions_won))
            else:
                NOT_FOUND += 1
#                f1.write('%s, DATA NOT AVAILABLE \n'%(bidder_line[0]))
                continue
    print NOT_FOUND
    f1.close()

def main():
    if sys.argv[1]<2:
        print "Enter the bids file and the bidder file"
        exit(0)
    
    bids_file = sys.argv[1]
    bidder_file = sys.argv[2]

    # extracting filename, extensions...
    (dirName, bidderName) = os.path.split(bidder_file)
    (bidderBaseName, bidderExtension)=os.path.splitext(bidder_file)

#    print dirName         # /Users/t/web/perl-python
#    print fileName        # banknote.txt
#    print fileBaseName    # banknote
#    print fileExtension   # .txt


    print "Working on "+bidderBaseName+" file" # working on test/train files...

#   making a folder to save the subsequent files
    if not os.path.exists(bidderBaseName):
        os.makedirs(bidderBaseName)
    newpath = bidderBaseName # either test or train
#   calling the function to make a separate file for each bidder and extract the corresponding records from the bids directory
    if EXTRACTED == 0:
        bidder_dict = extract_dict(bids_file)
#   function which goes through the test/train folder and builds features to be used for training 
    build_features_dict(bidder_file, newpath)

if __name__=="__main__":
    main()
