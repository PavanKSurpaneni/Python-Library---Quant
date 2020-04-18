# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:21:41 2020

@author: Pavankumar
"""


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