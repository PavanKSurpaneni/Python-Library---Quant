# -*- coding: utf-8 -*-
"""
Created on Sat Oct 12 19:41:38 2019

@author: ps294
"""

import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay as bday
from bisect import bisect_left as blf
from bisect import bisect_right as blr
import os

out='C:\\Users\\ps294\\OneDrive\\Desktop\\tool-creator\\'

if not os.path.isdir(out):
    os.makedirs(out)
#%% Function to get continuous futures return based on roll policy - 7 business days before First Notice- Roll 33% at a time from front to second futures cont.  
def get_continuous_return(): 
    fnd=pd.read_excel('C:\\Users\\ps294\\OneDrive\\Desktop\\XXX.xlsx',sheet_name='fnd')
    prc=pd.read_excel('C:\\Users\\ps294\\OneDrive\\Desktop\\XXXX2.xlsx',sheet_name='price')
    
    fnd['First Notice Day']=pd.to_datetime(fnd['First Notice Day'],format='%Y-%m-%d')
    fnd['roll_start_date']=[ x-bday(7) for x in list(fnd['First Notice Day']) ]
    
    roldates,tickers=fnd['roll_start_date'].tolist(),fnd['Ticker'].tolist()
    
    prc['date']=pd.to_datetime(prc['date'],format='%Y-%m-%d')
    prc['roll']=[ 0 if (  (i==0) or (blr(roldates, prc['date'].iloc[i])==blr(roldates, prc['date'].iloc[i-1])) ) else 1 for i in range(prc.shape[0]) ]
    prc['roll']=[ prc['roll'].iloc[i] if (i in [0,1]) or (prc['roll'].iloc[i-1]!=1) else 2 for i in range(prc.shape[0])   ]    
    prc['front']= [ tickers[blf(roldates, prc['date'].iloc[i])] for i in range(prc.shape[0])  ]
    prc['second']= [ tickers[blf(roldates, prc['date'].iloc[i])+1] for i in range(prc.shape[0])  ]
    
    prc['front']=[ prc['front'].iloc[i] if i==0 or prc['roll'].iloc[i]!=2 else prc['front'].iloc[i-1] for i in range(prc.shape[0])]
    prc['second']=[ prc['second'].iloc[i] if i==0 or prc['roll'].iloc[i]!=2 else prc['second'].iloc[i-1] for i in range(prc.shape[0])]
    
    prc['first_ret'] = [ 0 if i==0 else 100*(prc[prc['front'].iloc[i]].iloc[i]/prc[prc['front'].iloc[i]].iloc[i-1] - 1) for i in range(prc.shape[0]) ] 
    prc['second_ret'] = [ 0 if i==0 else 100*(prc[prc['second'].iloc[i]].iloc[i]/prc[prc['second'].iloc[i]].iloc[i-1] - 1) for i in range(prc.shape[0]) ]    
    prc['return'] = [ prc['first_ret'].iloc[i] if prc['roll'].iloc[i]==0 else  ( (3-prc['roll'].iloc[i])/3)*prc['first_ret'].iloc[i]+(prc['roll'].iloc[i]/3)*prc['second_ret'].iloc[i] for i in range(prc.shape[0]) ]
    
    prc.set_index('date',inplace=True)
    prc['return'].to_csv(f'{out}continuous_returns.csv')
    return

def monthly_rets_vols(data,name):
    models=list(data['model'].unique())
    for mod in models:
        tmp_ret=pd.DataFrame(data[data['model']==mod].groupby('yr-month',sort=False)['pnl'].sum())
        tmp_ret.columns=[mod]
        tmp_ret.index.names=['Monthly Return']
        
        tmp_vol=pd.DataFrame(data[data['model']==mod].groupby('yr-month',sort=False)['pnl'].std()*np.sqrt(21))
        tmp_vol.columns=[mod]
        tmp_vol.index.names=['Monthly Volatility']
        
        if mod ==models[0]:
            returns=tmp_ret.copy()
            vols=tmp_vol.copy() 
        else:
            returns=returns.join(tmp_ret)
            vols=vols.join(tmp_vol)
    
    returns.to_csv(f'{out}fund_{name}_monthly_returns.csv')    
    vols.to_csv(f'{out}fund_{name}_monthly_vols.csv')
    return

def pnl_attri():
    pnl=pd.read_excel('C:\\xxx.xlsx',sheet_name='pnl_eod')
    futs=pd.read_excel('C:\\xxx.xlsx',sheet_name='futures_models',usecols=[1],names=['name'],header=None)
    futs_list=futs['name'].tolist()
    
    pnl.dropna(axis=0,how='any',inplace=True)
    pnl['pnl']=pnl['pnl']/10e6
    pnl['yr-month']=pnl['date'].apply(lambda x: x.strftime('%b-%y') )
    
    spectrum=pnl[pnl['fund']=='sp'].copy()
    prism=pnl[pnl['fund']=='pr'].copy()
    prism['model'].replace(futs_list,'futures_model',inplace=True)
    
    monthly_rets_vols(prism,'pr')
    monthly_rets_vols(spectrum,'sp')     
    return

def main():
    get_continuous_return()
    pnl_attri()
    

main()
#%%
    
    
    
        
        