
"""
Created on Mon Dec 10 15:39:09 2018

@author: psurpaneni
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import os
from bisect import bisect_left as blf
from math import log
from datetime import timedelta
from bisect import bisect_right
import sys
import multiprocessing as mp

#%% Import path variables from ds001_config, import code for creating price files 

#Get all variables and code for creating price_files from minute bars
codedir='/'
sys.path.append(codedir)

from ds001_config import min_rep_path,rolspath,datespath,prcfls_path,featfls_path,dirpath,pit_time
from pricefiles_frm_mbars import get_mapping,gen_price,rollndates,get_cont,get_cont_ohlc

print (f'datespath is {datespath}')
BATCH_MODE = False
#%% Check if a symbol and it's related products' price file exists, if not this creates them
# This function uses gen_price function from pricefiles_frm_mbars.py file (which internally checks if the file exists at the path, if not it creates)
def check_pf(ls,syms,optime,dirpath):
    sydf=pd.read_excel(dirpath+'info.xlsx',sheetname='XXX_universe')
    stdt_mp={ sydf['name'].iloc[i]:max(sydf['Product-START'].iloc[i],19950102) for i in range(sydf.shape[0]) }
    
    for e in ls:
        try:
            gen_price( [syms[e],optime,stdt_mp[e] ] )
        except:
            x=min(sydf[sydf['related_feasible'].str.contains(syms[e])]['RLP-START'])  # for symbols we aren't tradign but need as related product (for ex: URO) -> take all rows where URO is a related product, take the min. of all those symbols rlp-start date
            gen_price( [syms[e],optime,x ] )
            
    return ls
#%% This take a daily ohlc dataframe, check for wave rules to flag highs nd lows: it takes 2 days for a low / high to be included , coz. it see open-open of today and open-open of tomorrow to confirm, the extrema holds for the next day also
# The correct column says whether a detected extrema was correct on not by seeing if it held for next 2 days (for ex: a low was detected nd not breached for the next 2 days, it will be correct )
def wave_daily(dat):
    hldat=pd.DataFrame(columns=['indx','date','hilo','prc','open','high','low','close','start','know','correct','op1','hi1','lo1','cl1'])
    hlin=0
    doo=False
    loo=False
    hf='no'
    
    dshp=dat.shape[0]
    
    for i in range(dshp-2):
        ch=float(dat['high'].iloc[i])
        cl=float(dat['low'].iloc[i])
        co=float(dat['open'].iloc[i])
        cc=float(dat['close'].iloc[i])
        
        tm=dat.index[i]
        co1=float(dat['open'].iloc[i+1])
        ch1=float(dat['high'].iloc[i+1])
        cl1=float(dat['low'].iloc[i+1])
        cc1=float(dat['close'].iloc[i+1])
        
        if i==dshp-3:
            knw=dat.index[i+2] + timedelta(days=1)
        else:
            knw=dat.index[i+3]
            
        if (i>2):
            if (((dat['high'].iloc[i-3]-dat['low'].iloc[i-3])/(dat['close'].iloc[i-3]) )<-1.5) and ((ch>max(dat['high'].iloc[i-2],dat['high'].iloc[i-1])) and (ch>=dat['high'].iloc[i+1])):
                doo=True   
                
            if ( ((dat['high'].iloc[i-3]-dat['low'].iloc[i-3])/(dat['close'].iloc[i-3]) )>-1.5) and ((ch>max(dat['high'].iloc[i-3],dat['high'].iloc[i-2],dat['high'].iloc[i-1])) and (ch>=dat['high'].iloc[i+1])):
                doo=True
            
        elif (i==2) and ( (ch>max(dat['high'].iloc[i-2],dat['high'].iloc[i-1])) and (ch>=dat['high'].iloc[i+1]) ):
            doo=True
        elif (i==1) and ( (ch>dat['high'].iloc[i-1]) and (ch>=dat['high'].iloc[i+1]) ):
            doo=True
        elif (i==0) and (ch>dat['high'].iloc[i+1]):
            doo=True
        else:
            pass

        while doo:
            
            if ch>=dat['high'].iloc[i+2]:
                cor='yes'
            else:
                cor='no'
            
            
            hldat.loc[hlin]= [i,tm,'hi',ch,co,ch,cl,cc,dat.index[i+2],knw,cor,co1,ch1,cl1,cc1] 
            hlin+=1  
            hf='yes'
            doo=False
            
        if hf!='yes':
            if (i>2):
                if (((dat['high'].iloc[i-3]-dat['low'].iloc[i-3])/(dat['close'].iloc[i-3]) )>1.5) and ((cl<min(dat['low'].iloc[i-2],dat['low'].iloc[i-1])) and (cl<=dat['low'].iloc[i+1])):
                    loo=True
                     
                if (((dat['high'].iloc[i-3]-dat['low'].iloc[i-3])/(dat['close'].iloc[i-3]) )<1.5) and ((cl<min(dat['low'].iloc[i-3],dat['low'].iloc[i-2],dat['low'].iloc[i-1])) and (cl<=dat['low'].iloc[i+1])):
                    loo=True
                    
            elif (i==2) and ((cl<min(dat['low'].iloc[i-2],dat['low'].iloc[i-1])) and (cl<=dat['low'].iloc[i+1])):
                loo=True
            elif (i==1) and ((cl<dat['low'].iloc[i-1]) and (cl<=dat['low'].iloc[i+1])):
                loo=True
            elif (i==0) and (cl<=dat['low'].iloc[i+1]):
                loo=True
            else:
                pass
            
            while loo:
                if cl<=dat['low'].iloc[i+2]:
                    cor='yes'
                else:
                    cor='no'
                
                hldat.loc[hlin]= [i,tm,'lo',cl,co,ch,cl,cc,dat.index[i+2],knw,cor,co1,ch1,cl1,cc1] 
                hlin+=1  
                hf='yes'
                doo=False
                loo=False
            
        hf='no'

    return hldat  
#%% Takes a contract for ex: SYH18 a start nd end date, then uses get_cont_ohlc function (from the other python file)  which :  gets all minute bars for that contract between those dates, creates a ohlc daily from those minute bars using optm as opentime
# Note: The start date will be about 120 days before this contract became first, so it has all the history to see highs and lows
def data4_wave(ct,st,exp,td_ticker,sydates,all_dates,optm):
    #print ('here ' , ct , st , exp2)
    dd=get_cont_ohlc(ct,st,exp,td_ticker,sydates,all_dates,optime=optm)
    dd.rename(index=str,columns={'Date':'date','Open':'open','High':'high','Low':'low','Close':'close'},inplace=True)
    dd['date']=[x.date() for x in list(dd['date'])]
    dd.set_index('date',inplace=True)
    
    xtr=wave_daily(dd)
    
    return xtr,dd

#%% A helper function to count successive counts of a number from 1st index, stops at the first occurence of a number in list l other than num (Default :1 )
def cnt_ones(l,num=1):
    
    ln=len(l)
    cnt=0
    for i in range(ln):
        if l[i]!=num:
            break
        else:
            cnt+=1
    return cnt
#%% Returns a non-zero number when a number is 0, used for denominators in fractions to avoid infs
def nzero(r):
    if r ==0:
        return 0.001
    else:
        return r


def ls3_cnt(ls):  # count all positive numbers in a list
    cnt=0
    for e in ls:
        if e>0:
            cnt+=1
    return cnt    

#%% It gets ohlcs for each day of symbol nd it's related products using the price file nd getting info for one day at a time
def get_ohlcs(fs,date,sypath,rlpls,flspath,open_time,ticker):  
    i=len(fs['opn'])-1
    
    raw=pd.read_csv(sypath,sep='\t',skiprows=1)
    raw['Date']=[int(x) for x in list(raw['Date'])]
    
    tpd=raw[raw['Date']==date].copy()
    tpd2= raw[raw['Date']<date].copy()
    
    fs['opn'].append(tpd['Open'].iloc[0])
    fs['close'].append(tpd['Close'].iloc[0])
    fs['hi'].append(tpd['High'].iloc[0])
    fs['lo'].append(tpd['Low'].iloc[0])

    if i>0:    
        fs['y'].append(tpd['PC(%)'].iloc[0])
        fs['ylg'].append( log(tpd2['Close'].iloc[-1]/ tpd2['Open'].iloc[-1]  )  )
        #print (date, tpd['Open'].iloc[0],tpd['High'].iloc[0],tpd['Low'].iloc[0],tpd['Close'].iloc[0] )
    else:
        fs['y'].append(np.nan)
        fs['ylg'].append(np.nan) 
    
    for e in range(len(rlpls)):
        tmp=pd.read_csv(f'{flspath}{rlpls[e]}_{open_time}.price',sep='\t',skiprows=1)
        tmp=tmp[tmp['Date']<date].copy()
           
        if e==0:
            if i>1:
                fs['rlp_open1'].append(tmp['Close'].iloc[-1])
            else:
                fs['rlp_open1'].append(np.nan)

        
        if i>1:
            fs['rlp_ret'+str(e)].append( log(tmp['Close'].iloc[-1]/ tmp['Open'].iloc[-1]  ) )
        else:
            fs['rlp_ret'+str(e)].append(np.nan)
#%%
# RELATED PRODUCTS   - It creates all the features based on related symbols , uses largely the log returns : rlp_ret_n (created in get_ohlcs fucntion) of symbol nd related products to create these features  
def rlp_feats(fs,rlpls,date,ticker):
    i=len(fs['opn'])-1
    #print ( ' i is ', i)
    if i>30:
        # Correlation based related products indicators  
        cor5s=[]
        cor20s=[]
           
        for e in range(len(rlpls)):
            he5=np.corrcoef(fs['ylg'][-5:],fs['rlp_ret'+str(e)][-5:] )[0][1]     # get correlation coefficient between product nd each of its related product - last 5 days, 20 days
            he20=np.corrcoef(fs['ylg'][-20:],fs['rlp_ret'+str(e)][-20:] )[0][1] 
            
            cor5s.append( round(he5,5) )   
            cor20s.append( round(he20,5) )
            
            #print (i,date,cor20s[-1])
            fs['rlp_cor5_'+str(e)].append(cor5s[-1] )            
            fs['rlp_cor20_'+str(e)].append( cor20s[-1] )
            
        fs['rlp_coravg5'].append( np.nanmean(cor5s) )             
        fs['rlp_coravg20'].append( np.nanmean(cor20s) )
            
        # Max. Min returns range based related products indicators  
        rlp_ys=[ fs['rlp_ret'+str(n)][-1] for n in range(len(rlpls)) ]
        
        ysmx= max(rlp_ys)
        ysmn=min(rlp_ys)
        
        if (ysmn==0) and (ysmx==0):
            ysmx=1
        
        if ysmn==ysmx:
            fs['rlp_ysthlp'].append(0)
        else:
            fs['rlp_ysthlp'].append( (fs['ylg'][-1]-ysmn)/(ysmx-ysmn) )
        
        rlp_5s= [ np.nansum(fs['rlp_ret'+str(n)][-5:]) for n in range(len(rlpls)) ]
        rlp_10s= [ np.nansum(fs['rlp_ret'+str(n)][-10:]) for n in range(len(rlpls)) ]
        rlp_25s= [ np.nansum(fs['rlp_ret'+str(n)][-25:]) for n in range(len(rlpls)) ]
        
        fs['rlp_5dhlp'].append (   ( np.nansum(fs['ylg'][-5:])-np.nanmin(rlp_5s) ) / (np.nanmax(rlp_5s)-np.nanmin(rlp_5s))  )
        fs['rlp_10dhlp'].append( ( np.nansum(fs['ylg'][-10:])-np.nanmin(rlp_10s)) /(np.nanmax(rlp_10s)-np.nanmin(rlp_10s)) ) 
        fs['rlp_25dhlp'].append( (np.nansum(fs['ylg'][-25:])-np.nanmin(rlp_25s) )/(np.nanmax(rlp_25s)-np.nanmin(rlp_25s)) ) 
          
        # Trailing returns differences based related products indicators 
        for e in range(len(rlpls)):
            fs['rlp_ysrdif'+str(e)].append (  fs['ylg'][-1] -fs['rlp_ret'+str(e)][-1] )
            fs['rlp_5drdif'+str(e)].append( np.nansum(fs['ylg'][-5:])-np.nansum(fs['rlp_ret'+str(e)][-5:]) )
            fs['rlp_20drdif'+str(e)].append( np.nansum(fs['ylg'][-20:])-np.nansum(fs['rlp_ret'+str(e)][-20:]) )
            
        # Ratio based 
        fs['rlp_syr'].append( round( fs['close'][-2]/fs['rlp_open1'][-1] ,9) )  
        
    else:
        for e in range(len(rlpls)):
            fs['rlp_cor5_'+str(e)].append(np.nan )
            fs['rlp_cor20_'+str(e)].append(np.nan)
            fs['rlp_ysrdif'+str(e)].append(np.nan)
            fs['rlp_5drdif'+str(e)].append(np.nan)
            fs['rlp_20drdif'+str(e)].append(np.nan)
        
        fs['rlp_coravg5'].append(np.nan)
        fs['rlp_coravg20'].append(np.nan)
        fs['rlp_ysthlp'].append(np.nan)
        fs['rlp_5dhlp'].append(np.nan)
        fs['rlp_10dhlp'].append(np.nan)
        fs['rlp_25dhlp'].append(np.nan)
        
        fs['rlp_syr'].append(np.nan)
     
        
    if i>60:
        fs['rlp_syrz20'].append( (fs['rlp_syr'][-1]-np.nanmean(fs['rlp_syr'][-20:]) ) / (np.nanstd(fs['rlp_syr'][-20:]) ) )
    else:
        fs['rlp_syrz20'].append(np.nan)
        
        
    if i>=1000:
        fs['rlp_syrzmx'].append( (fs['rlp_syr'][-1]-np.nanmean(fs['rlp_syr'][-1000:]) )/ (np.nanstd(fs['rlp_syr'][-1000:]) ) )
    elif i>=80:  
        fs['rlp_syrzmx'].append( (fs['rlp_syr'][-1]-np.nanmean(fs['rlp_syr'][-i:] ) )/ (np.nanstd(fs['rlp_syr'][-i:] )  ) )       
    else:
        fs['rlp_syrzmx'].append(np.nan)
  
#%%  The set of features which are wave based but generic i.e not specially aimed at just peaks or troughs
# 
def al_feats(fs,dd,xtr,nw,ticker):
    al=xtr[((xtr['start']<=nw) & (xtr['know']>nw)) | ( (xtr['know']<=nw) & (xtr['correct']=='yes'))].copy()
    #print (nw, 'al ' ,'\n',al)
    if al.shape[0]>0:
        ls=al.iloc[-1,:].copy()
    
        fs['wdl_bar'].append((ls['close']-ls['open'])/(ls['high']-ls['low']) )
        fs['wdl_rg'].append( (ls['high']-ls['low'])/1.0 )
        fs['wdl_ty'].append(ls['hilo'])
        fs['wdl_rgn'].append( (ls['hi1']-ls['lo1'])/1.0 ) 
        
        if ls['hi1']==ls['lo1']:
            #print (ticker, ' wdl_barn err ',nw)
            #print (al)
            fs['wdl_barn'].append(0)
        else:
            fs['wdl_barn'].append((ls['cl1']-ls['op1'])/(ls['hi1']-ls['lo1']))
           
        
        me=dd[(dd.index>ls['date'])&(dd.index<nw)].shape[0]

        if ls['hilo']=='hi':
            fs['wdl_tme'].append(me)
        else:
            fs['wdl_tme'].append(-1*me)
    else:
        ls=None
        me=None
        fs['wdl_bar'].append(np.nan)
        fs['wdl_rg'].append(np.nan)
        fs['wdl_ty'].append(np.nan)
        fs['wdl_barn'].append(np.nan)
        fs['wdl_rgn'].append(np.nan)
        fs['wdl_tme'].append(np.nan)
    
    if al.shape[0]>1:
        mlg=dd[(dd.index>al.iloc[-2,:]['date'])&(dd.index<=ls['date'])].shape[0]

        fs['wdl_tmlg'].append(mlg)
    else:
        fs['wdl_tmlg'].append(np.nan)      
    
    return al,ls,me
#%% All features which are peak based and need atleast 1 peak to be detected so far
def peak_feats(fs,al,dd,nw):
    #HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH
    h=al[al['hilo']=='hi'].copy()        # al has all extrema until today, just get all the peaks until today
    h['glhi']=h['prc'].cummax().shift(1) # get the global hi i.e highest peak high value we detected so far
    
    hlst=[1 if h['prc'].iloc[x]>=h['glhi'].iloc[x] else 0 for x in range(h.shape[0])]   # Mark all highs as 1 if it is higher than the prevailing global high unti then
    h['prv1']=h['prc'].shift(1) 
    h['hh']=[1 if h['prc'].iloc[i]>=h['prv1'].iloc[i] else 0 for i in range(h.shape[0])]  #  Mark all highs as 1 if it is higher than the previous high
    h['lh']=[1 if h['prc'].iloc[i]<h['prv1'].iloc[i] else 0 for i in range(h.shape[0])]    # Do now for lower highs
    
    fs['wdl_hbc'].append(cnt_ones(hlst[::-1]))      # Count n: where n is the last n is successive global highs 
    fs['wdl_lhc'].append(cnt_ones(list(h['lh'])[::-1]))  # Count n: where n is the no. of successive higher peaks (compared to previous peak)
    
    if h.shape[0]>0:
        lh=h.iloc[-1,:].copy()    # Get the last peak
        
        ht=lh['date']              # date of last peak, price of last peak
        hp=lh['prc']
    
        fs['wdl_dth'].append((fs['opn'][-1]-hp)/1.0)      # Calculate the distance from current open price to last high price
        fs['wdl_cdth'].append((fs['opn'][-1]-lh['close'])/nzero(fs['wdl_dth'][-1]))    # to understand peaks which have huge wicks, calculate open to last peak day close price / open to last peak day high price
        
        lah=dd[(dd.index>ht) &(dd.index<nw)]['low'].min()  # get all the days in between last peak day nd today, find the lowest low in between these
        
        fs['wdl_hbd'].append( (hp-lah)/1.0)            # get distance from last peak price to min price between that day nd today
        fs['wdl_hbn'].append( (fs['opn'][-1]-lah)/1.0)  # open price to that min. price between last peak day and today
        fs['wdl_hrt'].append( (fs['opn'][-1]-lah)/nzero(hp-lah) )  # get ratio of above two
        
        ahi=dd[(dd.index>=ht) &(dd.index<nw)].copy()    # Get all days between last peak day nd today
        ahi['prhi']=ahi['high'].shift(1)         
        ahi['prlo']=ahi['low'].shift(1)
        ahi=ahi.iloc[1:,:]
        
        tot=ahi.shape[0]
        hh=ahi[ahi['high']>ahi['prhi']].shape[0]    # counts of days between last peak nd today when high of day was higher than previous day high
        lh=ahi[ahi['high']<=ahi['prhi']].shape[0]   # counts of days between last peak nd today when high of day was lower than previous day high
        ll=ahi[ahi['low']<ahi['prlo']].shape[0]     # counts of days between last peak nd today when low of day was lower than previous day low
        hl=ahi[ahi['low']>=ahi['prlo']].shape[0]    # counts of days between last peak nd today when low of day was higher than previous day low
        fs['wdl_pkhh'].append((hh+1)/(tot+1) )   
        fs['wdl_pklh'].append((lh+1)/(tot+1) )
        fs['wdl_pkll'].append((ll+1)/(tot+1) )
        fs['wdl_pkhl'].append((hl+1)/(tot+1) )
        fs['wdl_pkud'].append( (hh+1)/(ll+1))

    else:
        hp=None
        ht=None
        fs['wdl_dth'].append(np.nan)
        fs['wdl_cdth'].append(np.nan)
        fs['wdl_hbd'].append(np.nan)
        fs['wdl_hbn'].append(np.nan)
        fs['wdl_hrt'].append(np.nan)
        
        fs['wdl_pkhh'].append(np.nan)
        fs['wdl_pklh'].append(np.nan)
        fs['wdl_pkll'].append(np.nan)
        fs['wdl_pkhl'].append(np.nan)
        fs['wdl_pkud'].append(np.nan)
        
        
    if h.shape[0]>1:                    # needs atleast last 2 highs
        lh2=h.iloc[-2,:].copy()            
        ht2=lh2['date']             # date and price of second last high
        hp2=lh2['prc']          
        fs['wdl_psth'].append( dd[(dd.index>ht2)&(dd.index<=ht)].shape[0])   # no. of days between last 2 peaks
        fs['wdl_psdh'].append( (hp-hp2)/1.0)            # distance between last 2 peaks
        hmid=dd[(dd.index>ht2)&(dd.index<ht)]['low'].min()    # between the last 2 peaks, lowest price

    else:
        hmid=None
        hp2=None
        fs['wdl_psth'].append(np.nan)
        fs['wdl_psdh'].append(np.nan)
        
    if h.shape[0]>2:
        lh3=h.iloc[-3,:].copy()
        hp3=lh3['prc']
        
        if (hp<=hp3) and (hp>hp2):
            fs['wdl_h3pat'].append(1)
        elif (hp>hp3) and (hp>hp2):
            fs['wdl_h3pat'].append(2)
        elif (hp<=hp3) and (hp>hp2):
            fs['wdl_h3pat'].append(3)
        else:
            fs['wdl_h3pat'].append(4)  
    else:
        fs['wdl_h3pat'].append(np.nan)   
        
    if h.shape[0]>=5:      # if there are atleast 5 peaks
        hred=h.iloc[-5:,:].copy() 
    else:
        hred=h.copy()
    
    #hred=h.iloc[1:,:]    
    fs['wdl_thhc'].append(hred['hh'].sum()/min(5,hred.shape[0]) )     # in the last 5 peaks , what % of them are higher peaks (compared to previous peak)
    fs['wdl_tlhc'].append(hred['lh'].sum()/min(5,hred.shape[0]) )   # % of lower peaks
    
    return hp,h,ht,hmid,hred,hp2
#HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH   
    
#%% All trough based features  
# LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL
def trough_feats(fs,al,dd,hp,hp2,hmid,nw,hred,ls):    
    l=al[al['hilo']=='lo'].copy()    # Get all the troughs detected so far in the wave system
    
    l['gllo']=l['prc'].cummin().shift(1)
    llst=[1 if l['prc'].iloc[x]<=l['gllo'].iloc[x] else 0 for x in range(l.shape[0])]
    l['prv1']=l['prc'].shift(1)
    
    tmpll=[1 if l['prc'].iloc[i]<=l['prv1'].iloc[i] else 0 for i in range(l.shape[0])]
    tmphl=[1 if l['prc'].iloc[i]>l['prv1'].iloc[i] else 0 for i in range(l.shape[0])]
    l['ll']=tmpll
    l['hl']=tmphl
    
    fs['wdl_lbc'].append(cnt_ones(llst[::-1]))
    fs['wdl_llc'].append(cnt_ones(list(l['ll'])[::-1]))
    fs['wdl_hlc'].append(cnt_ones(list(l['hl'])[::-1]))
    
    if l.shape[0]>0:  # See if atleast 1 trough exists so far for this contract
        ll=l.iloc[-1,:].copy()
        
        lt=ll['date']
        lp=ll['prc']
        
        fs['wdl_dtl'].append((fs['opn'][-1]-lp)/1.0)
        fs['wdl_cdtl'].append((fs['opn'][-1]-l.iloc[-1,:]['close'])/nzero(fs['wdl_dtl'][-1]))
        
        hal= dd[(dd.index>lt) &(dd.index<nw)]['high'].max()
        #print (nw, hal,lt, lp)
        fs['wdl_lbd'].append((hal-lp)/1.0)
        fs['wdl_lbn'].append((hal-fs['opn'][-1])/1.0)
        fs['wdl_lrt'].append( (hal-fs['opn'][-1])/ nzero(hal-lp) )
        
        alo=dd[(dd.index>=lt) &(dd.index<nw)].copy()
        alo['trhi']=alo['high'].shift(1)
        alo['trlo']=alo['low'].shift(1)
        alo=alo.iloc[1:,:]
 
        tol=alo.shape[0]
        hh=alo[alo['high']>alo['trhi']].shape[0]
        lh=alo[alo['high']<=alo['trhi']].shape[0]
        ll=alo[alo['low']<alo['trlo']].shape[0]
        hl=alo[alo['low']>=alo['trlo']].shape[0]
        
        fs['wdl_trhh'].append((hh+1)/(tol+1))
        fs['wdl_trlh'].append((lh+1)/(tol+1) )
        fs['wdl_trll'].append((ll+1)/(tol+1) )
        fs['wdl_trhl'].append((hl+1)/(tol+1) )
        fs['wdl_trud'].append((hh+1)/(ll+1))
        
    else:
        fs['wdl_cdtl'].append(np.nan)
        fs['wdl_dtl'].append(np.nan)
        fs['wdl_lbd'].append(np.nan)
        fs['wdl_lbn'].append(np.nan)
        fs['wdl_lrt'].append(np.nan)
        
        fs['wdl_trhh'].append(np.nan)
        fs['wdl_trlh'].append(np.nan)
        fs['wdl_trll'].append(np.nan)
        fs['wdl_trhl'].append(np.nan)
        fs['wdl_trud'].append(np.nan)
        
        
    if l.shape[0]>1: # See if atleast 2 troughs exist so far for this contract
        ll2=l.iloc[-2,:].copy()
        lt2=ll2['date']
        lp2=ll2['prc']
        lmid=dd[(dd.index>lt2)&(dd.index<lt)]['high'].max()    
        fs['wdl_rths'].append((lmid-lp2)/(lp-lmid))
        fs['wdl_pstl'].append(dd[(dd.index>lt2)&(dd.index<=lt)].shape[0])
        fs['wdl_psdl'].append((lp-lp2)/1.0)
    else:
        lt=None
        lp=None
        fs['wdl_rths'].append(np.nan)
        fs['wdl_pstl'].append(np.nan)
        fs['wdl_psdl'].append(np.nan)
    
    if l.shape[0]>2:
        ll3=l.iloc[-3,:].copy()
        lp3=ll3['prc']
    
        if (lp>=lp3) and (lp<lp2):
            fs['wdl_l3pat'].append(1)
        elif (lp<lp3) and (lp<lp2):
            fs['wdl_l3pat'].append(2)
        elif (lp>=lp3) and (lp>=lp2):
            fs['wdl_l3pat'].append(3)
        else:
            fs['wdl_l3pat'].append(4)
    else:
        if (l.shape[0]>1) and (lp<lp2):
            fs['wdl_l3pat'].append(2)
        elif (l.shape[0]>1) and (lp>=lp2):
            fs['wdl_l3pat'].append(3)
        else:
            fs['wdl_l3pat'].append(np.nan)
    
    if ls['hilo']=='lo':
        fs['wdl_lwcw'].append((fs['opn'][-1]-lp)/nzero(lmid-lp2))
        fs['wdl_hwcw'].append(-10)
    else:
        fs['wdl_hwcw'].append((fs['opn'][-1]-hp)/nzero(hmid-hp2))
        fs['wdl_lwcw'].append(-10)
    
    if l.shape[0]>5:
        lred=l.iloc[-5:,:].copy()
    else:
        lred=l.copy()
        
    fs['wdl_tllc'].append(lred['ll'].sum()/min(5,lred.shape[0]) )
    fs['wdl_thlc'].append(lred['hl'].sum()/min(5,lred.shape[0]) )
    fs['wdl_thhll'].append( (hred['hh'].sum()+1)/(lred['ll'].sum()+1) )
    fs['wdl_thr'].append( (hred['hh'].sum()+1)/ (hred['lh'].sum()+1) )    
        
    return lt,lp,l
#LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL  

#%% Set of features which use both the peaks and troughs information
def feats_wv_misc(fs,dd,xtr,h,l,nw,hp,lp,ht,lt,me,ticker):
    
    i=len(fs['opn'])-1
    
# Based on global highs and lows 
    gmn= dd[dd.index<nw]['low'].min()

    if fs['opn'][-1]<gmn:
        glcum=dd[dd.index<nw]['low'].idxmin()
        fs['wdl_tmgl'].append(dd[(dd.index>glcum)&(dd.index<nw)].shape[0])
        fs['wdl_gldt'].append(gmn-fs['opn'][-1])
    else:
        fs['wdl_tmgl'].append(-1)
        fs['wdl_gldt'].append(-1)
        
    gmx=dd[dd.index<nw]['high'].max()
    
    if fs['opn'][-1]>gmx:                              # Find the time,distance from the last global high
        ghcum=dd[dd.index<nw]['high'].idxmax()
        fs['wdl_tmgh'].append(dd[(dd.index>ghcum)&(dd.index<nw)].shape[0])
        fs['wdl_ghdt'].append((fs['opn'][-1]-gmx)/gmx)    
    else:
        fs['wdl_tmgh'].append(-1)
        fs['wdl_ghdt'].append(-1) 
    
    fs['wdl_glrng'].append ( (fs['opn'][-1]-gmn)/(gmx-gmn) ) # in the range of global high and low for this contract where are we now 
    
    # H & L : No. of days in the last 20 days when we were above global high or below global low for the contract   
    gbhc=h[(h['prc']>=h['glhi']) &( (h['date']>(nw-timedelta(days=20))) & (h['date']<nw) )].shape[0]
    gblc=l[ ( l['prc']<=l['gllo'] ) & ((l['date']>(nw-timedelta(days=20))) & (l['date']<nw) )].shape[0]
    fs['wdl_gbhlr'].append( (gbhc+1)/(gblc+1) )   
      
# End of indicators based on global highs and lows   
    
# based on existence of atleast a peak and a trough    
    if (l.shape[0]>0 and h.shape[0]>0):
        if lt<ht:
            gen=dd[(dd.index>lt)&(dd.index<=ht)].shape[0]  
            fs['wdl_ptl'].append(gen)
            fs['wdl_pts'].append((hp-lp)/1.0)
            fs['wdl_cpts'].append( (fs['opn'][-1]-hp)/nzero(hp-lp))
            fs['wdl_cptl'].append((me+1)/(gen+1))
            if ((fs['opn'][-1]-hp)/nzero(hp-lp))>-0.618:
                fs['wdl_trrev'].append(1)
            else:
                fs['wdl_trrev'].append(-1)
        else:
            gen2=dd[(dd.index>ht)&(dd.index<=lt)].shape[0]
            fs['wdl_ptl'].append(gen2)    
            fs['wdl_pts'].append( (lp-hp)/1.0)
            fs['wdl_cpts'].append((fs['opn'][-1]-lp)/nzero(lp-hp))
            fs['wdl_cptl'].append((me+1)/(gen2+1))
            
            if (lp==hp):
                fs['wdl_trrev'].append( fs['wdl_trrev'][-1] )         
                
            elif (fs['opn'][-1]-lp)/(lp-hp)>-0.618:
                fs['wdl_trrev'].append(-1)
        
            else:
                fs['wdl_trrev'].append(1)    
                
        
        if (lp==hp):
            fs['wdl_lhlp'].append(0)
        else:
            fs['wdl_lhlp'].append( (fs['opn'][-1]-lp)/(hp-lp))
            
    else:
        fs['wdl_ptl'].append(np.nan)
        fs['wdl_pts'].append(np.nan)
        fs['wdl_cpts'].append(np.nan)
        fs['wdl_cptl'].append(np.nan)
        fs['wdl_trrev'].append(np.nan)
        fs['wdl_lhlp'].append(np.nan)
        
# end of features based on existence of peak & trough            
 
# Features based on wrong peak identifications         
    if i>10:
        nwi=dd.index[dd.index.get_loc(nw)-10]   
        wrg=xtr[(xtr['know']<=nw) & ((xtr['correct']=='no') & (xtr['date']>nwi))].copy()
        
        #print (nw, 'wrg ' ,'\n',wrg)
        whi=wrg[wrg['hilo']=='hi'].shape[0]
        wlo=wrg[wrg['hilo']=='lo'].shape[0]
    else:
        whi=0
        wlo=0

    fs['wvw_pkh'].append(whi)  # no .of wrong peaks in last 10 days
    fs['wvw_trh'].append(wlo)   # no. of wrong troughs in last 10 days
    fs['wvw_wr'].append((whi+1)/(wlo+1)) # wrong peaks / wrong troughs counts in 10 days
#%% the daily candlesticks based features - higher highs , lower highs etc
def feats_dly(fs,dd,nw):
    i=len(fs['opn'])-1
    
    dd['hh']=[0 if ( (i==0) or (dd['high'].iloc[i]<=dd['high'].iloc[i-1]) ) else 1 for i in range(dd.shape[0]) ]
    dd['ll']=[0 if ( (i==0) or (dd['low'].iloc[i]>=dd['low'].iloc[i-1]) ) else 1 for i in range(dd.shape[0]) ]
    
    if i>24:
        tmp5=dd[dd.index<nw].iloc[-5:,:]    # last 5 days 
        tmp10=dd[dd.index<nw].iloc[-10:,:]   # last 10 days
        tmp20=dd[dd.index<nw].iloc[-20:,:]  # last 20 days
        
        fs['dly_hhcn5'].append( tmp5['hh'].sum() )  # count of higher highs 5 days
        fs['dly_hhcn10'].append( tmp10['hh'].sum() )  # count of higher highs 10 days
        fs['dly_hhcn20'].append( tmp20['hh'].sum() )  # count of higher highs 20 days
        
        fs['dly_llcn5'].append( tmp5['ll'].sum() )   # count of lower lows 5 days
        fs['dly_llcn10'].append( tmp10['ll'].sum() )  # count of lower lows 10 days
        fs['dly_llcn20'].append( tmp20['ll'].sum() )  # count of lower lows 20 days
        
        fs['dly_hhllcn5'].append( (tmp5['hh'].sum()+1)/(tmp5['ll'].sum()+1) )  # higher highs and lower lows  5 days 
        fs['dly_hhllcn10'].append( (tmp10['hh'].sum()+1)/(tmp10['ll'].sum()+1) )  # higher highs and lower lows  10 days
        fs['dly_hhllcn20'].append( (tmp20['hh'].sum()+1)/(tmp20['ll'].sum()+1) )   # higher highs and lower lows 20 dayss  
        
        fs['dly_2dyrt'].append(log(fs['opn'][-1]/fs['close'][-3]))     # 2 day log return
        fs['dly_5dyrt'].append(log(fs['opn'][-1]/fs['close'][-6]))   # 5 days log return
        fs['dly_10dyrt'].append(log(fs['opn'][-1]/fs['close'][-11]))  # 10 days log return
    else:
        fs['dly_hhcn5'].append(np.nan)
        fs['dly_hhcn10'].append(np.nan)
        fs['dly_hhcn20'].append(np.nan)
        fs['dly_llcn5'].append(np.nan)
        fs['dly_llcn10'].append(np.nan)
        fs['dly_llcn20'].append(np.nan)
        fs['dly_hhllcn5'].append(np.nan)
        fs['dly_hhllcn10'].append(np.nan)
        fs['dly_hhllcn20'].append(np.nan)
        fs['dly_2dyrt'].append(np.nan)
        fs['dly_5dyrt'].append(np.nan)
        fs['dly_10dyrt'].append(np.nan)
    
    sh= dd[dd.index<nw].shape[0]
    t1=dd[dd.index<nw].iloc[-min(1,sh),:]   # last day
    t2=dd[dd.index<nw].iloc[-min(2,sh),:]   # 2nd last days
    t3=dd[dd.index<nw].iloc[-min(3,sh),:]   # 3rd 
    t4=dd[dd.index<nw].iloc[-min(4,sh),:]   # 4th
    
    if i>6:
        fs['dly_gap'].append( (fs['opn'][-1]-t1['close']) )
    else:
        fs['dly_gap'].append(np.nan)
         
    if i>16:
        fs['dly_gapz'].append(  (fs['dly_gap'][-1] - np.nanmean(fs['dly_gap'][-6:-1]) )/ np.nanstd(fs['dly_gap'][-6:-1])  )
    else:
        fs['dly_gapz'].append( np.nan)
    
    #print ('t1c ', t1['close'],t1['open'],t1['high'], t1['low'])
    w1 = 0 if t1['high']==t1['low'] else (t1['close']-t1['open'])/(t1['high']-t1['low'])
    w2 = 0 if t2['high']==t2['low'] else (t2['close']-t2['open'])/(t2['high']-t2['low'])
    w3 = 0 if t3['high']==t3['low'] else (t3['close']-t3['open'])/(t3['high']-t3['low'])
    w4 = 0 if t4['high']==t4['low'] else (t4['close']-t4['open'])/(t4['high']-t4['low'])
    
    # can be further expanded into 4 cases
    if t1['low']<=min(fs['opn'][-1],t2['low'],t3['low'],t4['low']):
        fs['dly_lh3'].append(+1)
    elif t1['high']>=max(fs['opn'][-1],t2['high'],t3['high'],t4['high']):
        fs['dly_lh3'].append(-1)
    else:
        fs['dly_lh3'].append(0)
        
    fs['dly_wck1'].append( w1 )
    fs['dly_wck2'].append( w2 )
    fs['dly_wck3'].append( w3 )
    fs['dly_wck4'].append( w4 )
    fs['dly_wckav'].append( (w1+w2+w3+w4)/4)
    fs['dly_rge1'].append(t1['high']-t1['low'])
    fs['dly_rge2'].append(t2['high']-t2['low'])
    
    if t1['high']==t1['low']:
        fs['dly_pos'].append(0)
    else:
        fs['dly_pos'].append((t1['low']-fs['opn'][-1])/(t1['high']-t1['low']) )
        
        #print (nw,' fso ', fs['opn'][-1], ' t1lo ',t1['low'], ' ' ,t1['high'])
    
    if i>0:
        if fs['opn'][-1]>fs['close'][-2]:
            fs['dly_chl'].append(1)
        else:
            fs['dly_chl'].append(0)

    else:
        fs['dly_chl'].append(np.nan)
#%% Most of them are Moving average based features
def ma_dlymisc_feats(fs,dd,nw):
    i=len(fs['opn'])-1
    past=dd[dd.index<nw].copy()
    past['9m']=dd[dd.index<nw]['close'].rolling(window=9).mean()  # 9 dma
    past['20m']=dd[dd.index<nw]['close'].rolling(window=min(20,dd[dd.index<nw].shape[0])).mean()    # 20 dma
    
    if i>20 :
        fs['dly_dnm'].append( (fs['opn'][-1]-past['9m'].iloc[-1])) # distance from 9 dma
        fs['dly_dtwm'].append(fs['opn'][-1]-past['20m'].iloc[-1])  # distance from 20 dma
    else:
        fs['dly_dnm'].append(np.nan)
        fs['dly_dtwm'].append(np.nan)
    
    p9mab=[1 if fs['dly_dnm'][-min(30,i+1):][j]>=0 else -1 for j in range(min(30,i+1)) ]
    p20mab= [1 if fs['dly_dtwm'][-min(40,i+1):][j]>=0 else -1 for j in range(min(40,i+1)) ]
    
    fs['dly_dftm'].append(fs['opn'][-1]-dd[dd.index<nw]['close'].rolling(window=min(50,dd[dd.index<nw].shape[0] ) ).mean().iloc[-1])  
    fs['dly_dhdm'].append(fs['opn'][-1]-dd[dd.index<nw]['close'].rolling(window=min(100,dd[dd.index<nw].shape[0] ) ).mean().iloc[-1])  
    
    xl=p9mab[::-1][:min(25,len(p9mab)-1) ]
    yl=p20mab[::-1][:min(40,len(p20mab)-1) ]
    
    if p9mab[-1]==1:
        fs['dly_csnm'].append(cnt_ones(xl,1))
    else:
        fs['dly_csnm'].append(-1*cnt_ones(xl,-1))
    
    if p20mab[-1]==1:
        fs['dly_cstwm'].append(cnt_ones(yl,1))
    else:
        fs['dly_cstwm'].append(-1*cnt_ones(yl,-1))
        
    past['hidif1']=past['high']-past['high'].shift(1)    
    past['lodif1']=past['low']-past['low'].shift(1)

    if i>50:
        fs['dly_nmrt5'].append( (p9mab[-5:].count(1))/5)    # In the last 5 days count, how many of days we were above 9 dma
        fs['dly_twmrt5'].append( (p20mab[-5:].count(1))/5)    # In the last 5 days count, how many of days we were above 20 dma
        fs['dly_twmrt20'].append( (p20mab[-20:].count(1))/20)
        fs['dly_nmrt10'].append( (2*p9mab[-5:].count(1)+1) /(p9mab[-10:].count(1)+1) )
        fs['dly_nmrt20'].append( (p9mab[-20:].count(1))/20)
        fs['dly_twmrt10'].append( (2*p20mab[-5:].count(1)+1)/(p20mab[-10:].count(1)+1) )
        
        # trend of high difference (high (t) - high(t-1) ) and low differences in last 5 days
        fs['dly_arg5'].append( (past['high']-past['low']).rolling(window=5).mean().iloc[-1])
        fs['dly_hdif5'].append(past['hidif1'].iloc[-5:].mean() )     
        fs['dly_ldif5'].append(past['lodif1'].iloc[-5:].mean() ) 
        fs['dly_dfrt5'].append( past['hidif1'].iloc[-5:].mean() / nzero(past['lodif1'].iloc[-5:].mean()) )
        
        fs['dly_arg10'].append( (past['high']-past['low']).rolling(window=10).mean().iloc[-1])
        fs['dly_arg20'].append( ((past['high']-past['low']).rolling(window=3).mean().iloc[-1] )/( (past['high']-past['low']).rolling(window=20).mean().iloc[-1] ) ) 

    else:
        fs['dly_nmrt5'].append(np.nan)
        fs['dly_twmrt5'].append(np.nan)
        fs['dly_twmrt20'].append(np.nan)
        fs['dly_nmrt10'].append(np.nan)
        fs['dly_nmrt20'].append(np.nan)
        fs['dly_twmrt10'].append(np.nan)
        
        fs['dly_arg5'].append(np.nan)
        fs['dly_hdif5'].append(np.nan)
        fs['dly_ldif5'].append(np.nan)
        fs['dly_dfrt5'].append(np.nan)
        
        fs['dly_arg10'].append(np.nan)
        fs['dly_arg20'].append(np.nan)
    
    return past

#%% the second half is reveral based features, the first half is the ratio of returns of 3 largest red days to 3 largest green days in last 10,20 days
def ls_rvs_feats(fs,past):
    i=len(fs['opn'])-1
    past['oc']=(past['close']/past['open'] -1)*100
    
 # dly_ls10r &20r
    tmp=past['oc'].iloc[-20:].copy()      
    try:
        lg=sum(sorted(list(tmp[tmp>0.2]))[-3:])/3
    except:
        try:
            lg=sum(sorted(list(tmp[tmp>0.2]))[-2:])/2
        except:
            fs['dly_ls20r'].append(np.nan)

    if len(fs['dly_ls20r'])!=i+1:
        try:
            sh=sum(sorted(list(tmp[tmp<-0.2]))[::-1][-3:])/3
        except:
            try:
                sh=sum(sorted(list(tmp[tmp<-0.2]))[::-1][-2:])/2
            except:
                fs['dly_ls20r'].append(np.nan)
              
    if len(fs['dly_ls20r'])!=i+1:
        fs['dly_ls20r'].append((lg+0.05)/(sh+0.05))

    tmp=past['oc'].iloc[-10:].copy()      
 
    try:
        lg=sum(sorted(list(tmp[tmp>0.2]))[-2:])/2
    except:
        try:
            lg=sorted(list(tmp[tmp>0.2]))[-1:]
        except:
            fs['dly_ls10r'].append(np.nan)
    
    if len(fs['dly_ls10r'])!=i+1:
        try:
            sh=sum(sorted(list(tmp[tmp<-0.2]))[::-1][-2:])/2
        except:
            try:
                sh=sorted(list(tmp[tmp<-0.2]))[::-1][-1]
            except:
                fs['dly_ls10r'].append(np.nan)
        
            
    if len(fs['dly_ls10r'])!=i+1:
        fs['dly_ls10r'].append((lg+0.05)/(sh+0.05))     
    
    if (i>=8 and ( ls3_cnt(list(past['oc'][-5:-2])) <=1) ) and ( (past['oc'][-2]<0) and (past['oc'][-1]>=0) ):
        fs['rvs_d5'].append( -1*past['oc'][-1]/past['oc'][-2] )
        
    elif ( i>=8 and ( ls3_cnt(list(past['oc'][-5:-2])) >=2) ) and ( (past['oc'][-2]>0) and (past['oc'][-1]<=0) ):
        fs['rvs_d5'].append( past['oc'][-1]/past['oc'][-2] )
        
    else:
        fs['rvs_d5'].append(1000)
        
    
    if (i>=5 and ( past['oc'][-3] <0) ) and ( (past['oc'][-2]<0) and (past['oc'][-1]>=0) ):
        fs['rvs_d1'].append( -1*past['oc'][-1]/past['oc'][-2] )
        
    elif ( i>=5 and ( past['oc'][-3] >0) )  and ( (past['oc'][-2]>0) and (past['oc'][-1]<=0) ):
        fs['rvs_d1'].append( past['oc'][-1]/past['oc'][-2] )
        
    else:
        fs['rvs_d1'].append(1000)

#%% The time based features, time from last wasde, time from expiry, month of year, day of week etc.
def tme_wsd(ticker,fs,dd,nw,exp,rols,cont,wdates):
    fs['dates'].append(nw)
    expdt=pd.to_datetime(exp,format='%Y%m%d').date()
    fs['tme_expt'].append((expdt-nw).days)    
    nowsar=np.array(fs['dates'])
    
    exps=np.array([x.date() for x in pd.to_datetime(rols['expdate'],format='%Y%m%d') ])
    lexp=exps[exps<nw][-1]
    
    nowsar=np.array(fs['dates'])
    
    if lexp:
        fs['tme_lexp'].append( nowsar[(nowsar>=lexp)&(nowsar<nw)].shape[0])
    else:
        fs['tme_lexp'].append(np.nan)
    
    fs['tme_cont'].append(cont[-3])
    fs['tme_wn'].append(nw.day//7 +1 )
    fs['tme_dayow'].append(nw.weekday())
    fs['tme_mno'].append(nw.month)
    
    #if nw not in wdates:
    #    wddte=wdates[blf(wdates,nw)]
    #else:
    wddte=wdates[blf(wdates,nw)-1]
    
    wsday=dd[dd.index==wddte].copy()
    
    if ticker in ['C','S','W','FC','LC','LH','SM','BO']:
        if wsday.shape[0]==1:
            wscl=wsday['close'].iloc[0]
            wsop=wsday['open'].iloc[0]
            wshi=wsday['high'].iloc[0]
            wslo=wsday['low'].iloc[0]
            
            fs['tme_lwsd'].append( nowsar[(nowsar>=wddte)&(nowsar<nw)].shape[0])
            fs['ret_lwsd'].append(log(wscl/wsop))
            
            if (wshi==wslo):
                fs['wck_lwsd'].append(0)
            else:
                fs['wck_lwsd'].append( (wscl-wsop)/(wshi-wslo) )
            
        else:
            fs['ret_lwsd'].append(0)
            fs['wck_lwsd'].append(0)
            fs['tme_lwsd'].append(nowsar[(nowsar>=wddte)&(nowsar<nw)].shape[0])
#%%  This function gets the roll file and the wasde dates
def create_roll_wsd(fs,datespath,sypath,ticker,syms,iss):
    vp=pd.read_csv(datespath+syms[ticker]+'.csv')
    all_dates=list(vp['date'].unique())     
    pdts=pd.read_csv(sypath,sep='\t',skiprows=1)
    #print (pdts.head(5))
    sydates=[int(x) for x in list(pdts['Date'].unique() ) ]

    rols=pd.read_csv(rolspath+ticker.lower()+'.rolls',delimiter='\t',usecols=[0,1,2],header='infer',names=['cont','contyr','expdate'])
    ldate=rols['expdate'].iloc[len(rols)-2] 
    sydates=[ x for x in sydates if (x>=iss and x<=ldate) ]
    
    op=f'{featfls_path}{ticker}/{ticker}_feats.csv'
    sti=0
    if os.path.isfile(op):
        curfile=pd.read_csv(op)  
        cdate=curfile['dates'].iloc[-1]
        sti=bisect_right(sydates,cdate)

        if sti==len(sydates):
            return None,None,None,None,None,None,sydates,None,sti
        for e in fs:
            fs[e]=list(curfile[e])
            
        fs['dates']=[ pd.to_datetime(int(x),format='%Y%m%d').date() for x in list(fs['dates']) ]
        
    j=0
    while (j<=rols.shape[0]-1 ) and (rols['expdate'].iloc[j]<sydates[sti]):
        j+=1
    
    rid=j
    nln=len(ticker)+1
    yri=[len(str(x[nln:])) for x in list(rols['cont'] )]
    mnl=min(yri)
    
    if mnl==1:
        rols['cont']=[ rols['cont'].iloc[i][:nln] + str(rols['expdate'].iloc[i])[2:4] if rols['cont'].iloc[i][-1]==str(rols['expdate'].iloc[i])[3] else rols['cont'].iloc[i][:nln]+ str(int(str(rols['expdate'].iloc[i])[:4])+1)[2:4] for i in range(rols.shape[0]) ] 
    
    rols['cont']=[syms[ticker]+x[-3:] for x in list(rols['cont'])]
    exp=int(rols['expdate'].iloc[rid])
    cont=rols['cont'].iloc[rid]
        
    wsd=pd.read_csv(dirpath+'release_dates.csv',header='infer',names=['dates'])
    wsd['dates']=[x.date() for x in pd.to_datetime(wsd['dates'],format='%Y%m%d') ]
    wdates=list(wsd['dates'])
    
    return fs,rols,exp,cont,rid,wdates,sydates,all_dates,sti         
#%% Create a dictionary to hold feature-half names (for ex: barn here is wdl_barn)
def create_feat_dic(ticker,rlpls):
    fls=['dates','opn','hi','lo','close','rlp_open1','ylg','y'] # drop first 7 of these before training for backtests and drop all 8 (including 'y' when last row is used as test observation of live-trading)
    fls.extend(['wdl_bar','wdl_rg','wdl_ty','wdl_barn','wdl_rgn','wdl_tme','wdl_tmlg','wdl_dth','wdl_cdth','wdl_dtl','wdl_cdtl','wdl_hbd','wdl_lbd','wdl_hbn','wdl_lbn','wdl_hrt','wdl_lrt','wdl_pts','wdl_ptl','wdl_cpts','wdl_cptl','wdl_trrev','wdl_psth','wdl_psdh','wdl_pstl','wdl_psdl','wdl_h3pat'])
    fls.extend(['wdl_l3pat','wdl_rths','wdl_lwcw','wdl_hwcw','wdl_tmgh','wdl_tmgl','wdl_ghdt','wdl_gldt','wdl_hbc','wdl_lbc','wdl_lhc','wdl_llc','wdl_hlc','wdl_gbhlr','wdl_thhc','wdl_tlhc','wdl_tllc','wdl_thlc','wdl_thhll','wdl_thr','wdl_trhh','wdl_trlh','wdl_trll','wdl_trhl','wdl_trud','wdl_pkhh'])
    fls.extend(['wdl_pklh','wdl_pkll','wdl_pkhl','wdl_pkud','wdl_lhlp','wvw_pkh','wvw_trh','wvw_wr','dly_hhcn5','dly_hhcn10','dly_hhcn20','dly_llcn5','dly_llcn10','dly_llcn20','dly_hhllcn5','dly_hhllcn10','dly_hhllcn20'])
    fls.extend(['dly_gap','dly_gapz','dly_wck1','dly_wck2','dly_wck3','dly_wck4','dly_wckav','dly_chl','dly_pos','dly_dnm','dly_csnm','dly_nmrt5','dly_nmrt10','dly_nmrt20','dly_dtwm'])
    fls.extend(['dly_cstwm','dly_twmrt5','dly_twmrt10','dly_twmrt20','dly_dftm','dly_dhdm','dly_hdif5','dly_ldif5','dly_dfrt5','dly_rge1','dly_rge2','dly_arg5','dly_arg10','dly_arg20','dly_lh3','dly_ls20r','dly_ls10r'])
    fls.extend(['rvs_d5','rvs_d1','tme_expt','tme_lexp','tme_cont','tme_wn','tme_dayow','tme_mno'])
    fls.extend(['rlp_syrzmx','dly_2dyrt','dly_5dyrt','dly_10dyrt','wdl_glrng'])
    fls.extend(['rlp_coravg5','rlp_coravg20','rlp_ysthlp','rlp_5dhlp','rlp_10dhlp','rlp_25dhlp','rlp_syr','rlp_syrz20'])
    
    if ticker in ['C','S','W','FC','LC','LH','SM','BO']:
        fls.extend ( ['ret_lwsd','wck_lwsd','tme_lwsd'])
        
    for i in range(len(rlpls)):
        fls.extend(['rlp_ret'+str(i), 'rlp_cor5_'+str(i),'rlp_cor20_'+str(i),'rlp_ysrdif'+str(i),'rlp_5drdif'+str(i),'rlp_20drdif'+str(i) ])
    fs={}
    for e in fls:
        fs[e]=[]
    return fs
#%%
# Normalization of features & cleaning
def out_norm_clean(mdata,rlpls,ticker,sti):

    mdata['vol20']= mdata['y'].rolling(20).std().values
    abnorm=['wdl_psdh','wdl_lbn','dly_dhdm','wdl_dth','wdl_psdl','dly_dftm','dly_ldif5','wdl_hbn','dly_dtwm','dly_dnm','dly_hdif5','wdl_dtl','wdl_hbd','wdl_lbd','wdl_pts','wdl_rg','dly_rge2','dly_rge1','wdl_rgn','dly_arg5','dly_arg10']
    # Norm by dividing (vol*price)
    for e in abnorm:
        mdata[e].iloc[sti:]= mdata[e].iloc[sti:].values/(mdata['vol20'].iloc[sti:].values*mdata['close'].iloc[sti:].values) 
         
    mdata['dly_gap'].iloc[sti:]= mdata['dly_gap'].iloc[sti:].values/mdata['vol20'].iloc[sti:].values 

    for rl in range(len(rlpls)):
        mdata['rlp_ysrdif'+str(rl)].iloc[sti:]= mdata['rlp_ysrdif'+str(rl)].iloc[sti:].values /mdata['vol20'].iloc[sti:].values 
        
    for rl in range(len(rlpls)):
        mdata['rlp_5drdif'+str(rl)].iloc[sti:]=mdata['rlp_5drdif'+str(rl)].iloc[sti:].values /mdata['vol20'].iloc[sti:].values 
        mdata['rlp_20drdif'+str(rl)].iloc[sti:]=mdata['rlp_20drdif'+str(rl)].iloc[sti:].values /mdata['vol20'].iloc[sti:].values 
            
    mdata.drop('vol20',axis=1,inplace=True)
    mdata.fillna(0,inplace=True)
    mdata.replace(np.inf,10000,inplace=True)
    mdata.replace(-np.inf,-10000,inplace=True)
    
    #mdata['y']=mdata['y'].shift(-1)
    return mdata  
#%% Do the corrections in naming
# normalize for features the right way (some divided by price & vol )
# shift the y by -1 so we predict the next days return i.e at this open predict this open to next open return
# output the data
def output_data(fs,rlpls,ticker,sti):    
    feats=[x for x in list(fs.keys()) ]  # ['ylg','opn','close','hi','lo'] drop these 5 columns while training for backtests
    mdata=pd.DataFrame(columns=feats) 
    
    for f in feats: 
        mdata[f]=np.array(fs[f])
  
    mdata['vol20']= mdata['y'].rolling(20).std().values
    mdata['wdl_ty']=[ x if x in [1,-1] else 1 if x=='hi' else -1 for x in fs['wdl_ty'] ]
    cont_map={'F':0,'G':1,'H':2,'J':3,'K':4,'M':5,'N':6,'Q':7,'U':8,'V':9,'X':10,'Z':11}    
    mdata['tme_cont']= [ x if x in list(range(12)) else cont_map[x.upper()] for x in fs['tme_cont'] ]    
    mdata['dates']=[ int(str(x)[:4]+str(x)[5:7]+str(x)[8:10]) for x in list(mdata['dates'].values)]
    mdata.to_csv(f'{featfls_path}{ticker}/{ticker}_feats.csv',float_format='%.12f',index=False)
#%% Used to generate packets of symbol, related products, pit time, start date for all symbols and created a list of lists
def get_packets(dirpath):
    ans=[]
    sydf=pd.read_excel(dirpath+'sym_info.xlsx',sheetname='ds001_universe')
    
    symp=get_mapping(dirpath,'tickdata')
    
    for e in range(sydf.shape[0]):
        if sydf['name'].iloc[e] in ['W','C','S']:
            tmp=[sydf['name'].iloc[e]]
            tmp.extend( [symp[x] for x in list(sydf['related_feasible'].iloc[e].split(',')) ] )
            if len(str(sydf[pit_time].iloc[e]))==3:
                tmp.append(str('0'+str(sydf[pit_time].iloc[e]) ) )
            else:
                tmp.append(str(sydf[pit_time].iloc[e]) )
            tmp.append( max( sydf['Product-START'].iloc[e],sydf['RLP-START'].iloc[e],19950101 ) )
            ans.append( tmp)

    return ans
#%% The main fucntion which uses all the above functions, takes one day at a time and generates features for that date- add these values to the dictionary {feature names :  list of values for that feature }
def main_gen(item):
    ## INTENTIONALLY DELETED
    ## CONTENT MISSING                
    ## CONTENT MISSING
    ## INTENTIONALLY DELETED
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

