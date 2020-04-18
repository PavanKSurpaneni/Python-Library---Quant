# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:28:39 2020

@author: Pavankumar
"""


#%%
# Normalization of features & cleaning
def out_norm_clean(mdata,rlpls,ticker,sti):

    mdata['vol20']= mdata['y'].rolling(20).std().values
    abnorm=['wdl_psdh','wdl_lbn','dly_dhdm','wdl_dth','wdl_psdl','dly_dftm','dly_ldif5','wdl_hbn','dly_dtwm','dly_dnm','dly_hdif5','wdl_dtl','wdl_hbd','wdl_lbd','wdl_pts','wdl_rg','dly_rge2','dly_rge1','wdl_rgn','dly_arg5','dly_arg10']
    # Norm by dividing (vol*price)
    for e in abnorm:
        mdata[e].iloc[sti:]= mdata[e].iloc[sti:].values/(mdata['vol20'].iloc[sti:].values*mdata['close'].iloc[sti:].values) 
         
    mdata['dly_gap'].iloc[sti:]= mdata['dly_gap'].iloc[sti:].values/mdata['vol20'].iloc[sti:].values 

    for rl in range(len(rlpls)):
        mdata['rlp_ysrdif'+str(rl)].iloc[sti:]= mdata['rlp_ysrdif'+str(rl)].iloc[sti:].values /mdata['vol20'].iloc[sti:].values 
        
    for rl in range(len(rlpls)):
        mdata['rlp_5drdif'+str(rl)].iloc[sti:]=mdata['rlp_5drdif'+str(rl)].iloc[sti:].values /mdata['vol20'].iloc[sti:].values 
        mdata['rlp_20drdif'+str(rl)].iloc[sti:]=mdata['rlp_20drdif'+str(rl)].iloc[sti:].values /mdata['vol20'].iloc[sti:].values 
            
    mdata.drop('vol20',axis=1,inplace=True)
    mdata.fillna(0,inplace=True)
    mdata.replace(np.inf,10000,inplace=True)
    mdata.replace(-np.inf,-10000,inplace=True)
    
    #mdata['y']=mdata['y'].shift(-1)
    return mdata  