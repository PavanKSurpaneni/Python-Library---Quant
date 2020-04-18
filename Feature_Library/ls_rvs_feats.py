# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:25:22 2020

@author: Pavankumar
"""


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
