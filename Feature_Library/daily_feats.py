# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:23:00 2020

@author: Pavankumar
"""


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