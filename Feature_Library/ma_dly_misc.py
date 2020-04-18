# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:24:29 2020

@author: Pavankumar
"""


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