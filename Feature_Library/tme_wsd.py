# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:25:57 2020

@author: Pavankumar
"""


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