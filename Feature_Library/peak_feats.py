# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:19:41 2020

@author: Pavankumar
"""

# This function is used in main.py
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
    