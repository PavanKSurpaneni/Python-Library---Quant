# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:17:54 2020

@author: Pavankumar
"""


#%%
# RELATED PRODUCTS   - It creates all the features based on related symbols , uses largely the log returns : rlp_ret_n (created in get_ohlcs fucntion) of symbol nd related products to create these features  
def rlp_feats(fs,rlpls,date,ticker):
    i=len(fs['opn'])-1
    #print ( ' i is ', i)
    if i>30:
        # Correlation based related products indicators  
        cor5s=[]
        cor20s=[]
           
        for e in range(len(rlpls)):
            he5=np.corrcoef(fs['ylg'][-5:],fs['rlp_ret'+str(e)][-5:] )[0][1]     # get correlation coefficient between product nd each of its related product - last 5 days, 20 days
            he20=np.corrcoef(fs['ylg'][-20:],fs['rlp_ret'+str(e)][-20:] )[0][1] 
            
            cor5s.append( round(he5,5) )   
            cor20s.append( round(he20,5) )
            
            #print (i,date,cor20s[-1])
            fs['rlp_cor5_'+str(e)].append(cor5s[-1] )            
            fs['rlp_cor20_'+str(e)].append( cor20s[-1] )
            
        fs['rlp_coravg5'].append( np.nanmean(cor5s) )             
        fs['rlp_coravg20'].append( np.nanmean(cor20s) )
            
        # Max. Min returns range based related products indicators  
        rlp_ys=[ fs['rlp_ret'+str(n)][-1] for n in range(len(rlpls)) ]
        
        ysmx= max(rlp_ys)
        ysmn=min(rlp_ys)
        
        if (ysmn==0) and (ysmx==0):
            ysmx=1
        
        if ysmn==ysmx:
            fs['rlp_ysthlp'].append(0)
        else:
            fs['rlp_ysthlp'].append( (fs['ylg'][-1]-ysmn)/(ysmx-ysmn) )
        
        rlp_5s= [ np.nansum(fs['rlp_ret'+str(n)][-5:]) for n in range(len(rlpls)) ]
        rlp_10s= [ np.nansum(fs['rlp_ret'+str(n)][-10:]) for n in range(len(rlpls)) ]
        rlp_25s= [ np.nansum(fs['rlp_ret'+str(n)][-25:]) for n in range(len(rlpls)) ]
        
        fs['rlp_5dhlp'].append (   ( np.nansum(fs['ylg'][-5:])-np.nanmin(rlp_5s) ) / (np.nanmax(rlp_5s)-np.nanmin(rlp_5s))  )
        fs['rlp_10dhlp'].append( ( np.nansum(fs['ylg'][-10:])-np.nanmin(rlp_10s)) /(np.nanmax(rlp_10s)-np.nanmin(rlp_10s)) ) 
        fs['rlp_25dhlp'].append( (np.nansum(fs['ylg'][-25:])-np.nanmin(rlp_25s) )/(np.nanmax(rlp_25s)-np.nanmin(rlp_25s)) ) 
          
        # Trailing returns differences based related products indicators 
        for e in range(len(rlpls)):
            fs['rlp_ysrdif'+str(e)].append (  fs['ylg'][-1] -fs['rlp_ret'+str(e)][-1] )
            fs['rlp_5drdif'+str(e)].append( np.nansum(fs['ylg'][-5:])-np.nansum(fs['rlp_ret'+str(e)][-5:]) )
            fs['rlp_20drdif'+str(e)].append( np.nansum(fs['ylg'][-20:])-np.nansum(fs['rlp_ret'+str(e)][-20:]) )
            
        # Ratio based 
        fs['rlp_syr'].append( round( fs['close'][-2]/fs['rlp_open1'][-1] ,9) )  
        
    else:
        for e in range(len(rlpls)):
            fs['rlp_cor5_'+str(e)].append(np.nan )
            fs['rlp_cor20_'+str(e)].append(np.nan)
            fs['rlp_ysrdif'+str(e)].append(np.nan)
            fs['rlp_5drdif'+str(e)].append(np.nan)
            fs['rlp_20drdif'+str(e)].append(np.nan)
        
        fs['rlp_coravg5'].append(np.nan)
        fs['rlp_coravg20'].append(np.nan)
        fs['rlp_ysthlp'].append(np.nan)
        fs['rlp_5dhlp'].append(np.nan)
        fs['rlp_10dhlp'].append(np.nan)
        fs['rlp_25dhlp'].append(np.nan)
        
        fs['rlp_syr'].append(np.nan)
     
        
    if i>60:
        fs['rlp_syrz20'].append( (fs['rlp_syr'][-1]-np.nanmean(fs['rlp_syr'][-20:]) ) / (np.nanstd(fs['rlp_syr'][-20:]) ) )
    else:
        fs['rlp_syrz20'].append(np.nan)
        
        
    if i>=1000:
        fs['rlp_syrzmx'].append( (fs['rlp_syr'][-1]-np.nanmean(fs['rlp_syr'][-1000:]) )/ (np.nanstd(fs['rlp_syr'][-1000:]) ) )
    elif i>=80:  
        fs['rlp_syrzmx'].append( (fs['rlp_syr'][-1]-np.nanmean(fs['rlp_syr'][-i:] ) )/ (np.nanstd(fs['rlp_syr'][-i:] )  ) )       
    else:
        fs['rlp_syrzmx'].append(np.nan)
  