# -*- coding: utf-8 -*-
"""
@author: Pavankumar
"""


import numpy as np
import pandas as pd
import os
from bisect import bisect_left as blf
from math import log
from datetime import timedelta
from bisect import bisect_right
import sys
import multiprocessing as mp

import al_feats,create_feat_dic,create_roll_wsd,daily_feats,wave_nd_tools
import get_ohlcs,get_packets_output_data,ls_rvs_feats,ma_dlymisc_feats
import out_norm_clean,peak_feats,rlp_feats,tme_wsd,trough_feats,wave_misc_feats

#%% Import path variables from ds001_config, import code for creating price files 

#Get all variables and code for creating price_files from minute bars
codedir='/home/psurpaneni/alpha/commtech/code/'
sys.path.append(codedir)

from ds001_config import min_rep_path,rolspath,datespath,prcfls_path,featfls_path,dirpath,pit_time
from pricefiles_frm_mbars import get_mapping,gen_price,rollndates,get_cont,get_cont_ohlc

print (f'datespath is {datespath}')
BATCH_MODE = False

#%% The main fucntion which uses all the above functions, takes one day at a time and generates features for that date- add these values to the dictionary {feature names :  list of values for that feature }
def main_gen(item):
    print ('in main')
    ticker=item[0]   # ticker
    iss=item[-1]    # in-sample start
    rlpls= item[1:-2]  # all related products tickers
    open_time=item[-2]  # open time
    pricepath=f'{prcfls_path}{ticker}_{open_time}.price'
    
    syms=get_mapping(dirpath,'sct')   # Create a symbol dictionary map from sct symbol to tickdata symbol
    td_ticker=syms[ticker]    # tickdata ticker for this sct ticker
    
    check_pf(item[:-2],syms,open_time,dirpath)   # Create pricefiles for this ticker and it's related products tickers based for the open_time 
    fs=create_feat_dic(ticker,rlpls)         # create a dictionary to hold all features values with feaure names as keys nd list of feature values as value 
 
    # if feature file already existed, fs (feature dictionary) will include all the data from existing file and sydates will only include new dates for which file doesn't have data 
    #print('{}'.format(pricepath))
    fs,rols,exp,cont,rid,wdates,sydates,all_dates,sti = create_roll_wsd(fs,datespath,pricepath,ticker,syms,iss)

    ln=len(sydates)
    
    #print (' len sydates is ',ln)
    if sti==ln:
        print ('file present , no update needed')
        return 
    
    for i in range(sti,ln):
        date=sydates[i]
        print(ticker,date)
        sdate=str(date) 
        nw=pd.to_datetime(sdate,format='%Y%m%d').date()
        #if i%1501==0:
        #    print (ticker, nw)
        
        # Function 1
        get_ohlcs(fs,date,pricepath,rlpls,prcfls_path,open_time,ticker)
        rlp_feats(fs,rlpls,date,ticker)
        
        if (i==0) or (i==sti):
            
            if i==0:
                st=str(nw -timedelta(days=120))
                st= int(st[:4]+st[5:7]+st[8:10])
            else:
                st= [x for x in sydates if x>rols['expdate'][rid-1]][0]
                #print (' update contract first date of this contract is ', st)
                st= str(pd.to_datetime(st,format='%Y%m%d').date() -timedelta(days=120))
                st=int(st[:4]+st[5:7]+st[8:10])
                
            exp2=  str(pd.to_datetime(exp,format='%Y%m%d').date() +timedelta(days=10)) 
            exp2=int(exp2[:4]+exp2[5:7]+exp2[8:10])
            
            xtr,dd=data4_wave(cont,st,exp2,td_ticker,sydates,all_dates,open_time)
            
            #roll=0
            if not os.path.isdir(featfls_path+ticker+'/xtr/'):
                os.makedirs(featfls_path+ticker+'/xtr/') 
            #xtr.to_csv(featfls_path+ticker+'/xtr/xtr'+str(rid)+'_'+str(st)+'_'+str(exp)+'.csv')
            
        if date>exp:
            rid+=1
            cont=rols['cont'].iloc[rid]
            exp=rols['expdate'].iloc[rid]
            
            exp2=  str(pd.to_datetime(exp,format='%Y%m%d').date() +timedelta(days=10)) 
            exp2=int(exp2[:4]+exp2[5:7]+exp2[8:10])
            
            st=str(nw -timedelta(days=120))
            st= int (st[:4]+st[5:7]+st[8:10])
          
            xtr,dd=data4_wave(cont,st,exp2,td_ticker,sydates,all_dates,open_time)
            #xtr.to_csv(featfls_path+ticker+'/xtr/xtr'+str(rid)+'_'+str(st)+'_'+str(exp)+'.csv')
        
            
        al,ls,me = al_feats(fs,dd,xtr,nw,ticker) 
        
        hp,h,ht,hmid,hred,hp2 = peak_feats(fs,al,dd,nw)
        
        lt,lp,l =  trough_feats(fs,al,dd,hp,hp2,hmid,nw,hred,ls)
        
        feats_wv_misc(fs,dd,xtr,h,l,nw,hp,lp,ht,lt,me,ticker)
        
        feats_dly(fs,dd,nw)
          
        past=ma_dlymisc_feats(fs,dd,nw)
        
        ls_rvs_feats(fs,past)
        tme_wsd(ticker,fs,dd,nw,exp,rols,cont,wdates)
    
    output_data(fs,rlpls,ticker,sti)
    
    print ('ticker ', ticker ,'  done ')
#%%

if __name__ == '__main__':
    
    inn = input(' Menu, Enter a number from below \n  1 : generate feature file for a symbol of ur choice \n 2 : generate feature files for all symbols in ds001 strategy ')
    
    if int(inn)==2:
        packets=get_packets(dirpath)
        print (packets)
        p=mp.Pool(8)
        p.map(main_gen,packets)
    else:
        sym=input('Enter sct symbol in caps (S : for soy) for which u need the feature file for ')
        #po=input('Enter pit open time in 24hr format , ex: 0830 ')
        
        f=pd.read_excel(dirpath+'sym_info.xlsx',sheetname='ds001_universe')
        
        sct_to_td_mp ={f['name'].iloc[i]:f['tickdata-symbol'].iloc[i] for i in range(f.shape[0]) }
        td_to_sct_mp ={f['tickdata-symbol'].iloc[i]:f['name'].iloc[i] for i in range(f.shape[0]) }
        
        sdmp={ f['name'].iloc[i]:max(f['RLP-START'].iloc[i],f['Product-START'].iloc[i],19950101) for i in range(f.shape[0]) }
        stmp={ f['name'].iloc[i]:f[pit_time].iloc[i] for i in range(f.shape[0]) }
        
        start=f[f['name']==sym]['RLP-START']
        #sd=int(input ('Enter a date after {} for this symbol in yyyymmdd format , ex: 20120806 '.format(sdmp[sym])  ))
        #while sd<=int(sdmp[sym]):
        sd=int(input ('Enter a date after {} for this symbol in yyyymmdd format , ex: 20120806 '.format( sdmp[sym] ) ))
        if sd<sdmp[sym]:
            print (f'Entered too early date, enter a date after {sdmp[sym]} ')
        tmp= [sym]
        tmp.extend( [ td_to_sct_mp[x] for x in list(f[f['name']==sym]['related_feasible'].iloc[0].split(',') ) ])
        tmp.append(str('0'+str(f[f['name']==sym][pit_time].iloc[0]) ) )
        tmp.append( max( f[f['name']==sym]['Product-START'].iloc[0],f[f['name']==sym]['RLP-START'].iloc[0],19950102,sd) )
 
        main_gen(tmp)
