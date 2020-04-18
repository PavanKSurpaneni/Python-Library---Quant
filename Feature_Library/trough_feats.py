# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:20:48 2020

@author: Pavankumar
"""


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
