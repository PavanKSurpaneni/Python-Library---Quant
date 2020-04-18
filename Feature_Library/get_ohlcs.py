# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:16:51 2020

@author: Pavankumar
"""

# This function is used in main
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