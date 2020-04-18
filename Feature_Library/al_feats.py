# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:18:48 2020

@author: Pavankumar
"""


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