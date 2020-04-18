# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:28:02 2020

@author: Pavankumar
"""


#%% Create a dictionary to hold feature-half names (for ex: barn here is wdl_barn)
def create_feat_dic(ticker,rlpls):
    fls=['dates','opn','hi','lo','close','rlp_open1','ylg','y'] # drop first 7 of these before training for backtests and drop all 8 (including 'y' when last row is used as test observation of live-trading)
    fls.extend(['wdl_bar','wdl_rg','wdl_ty','wdl_barn','wdl_rgn','wdl_tme','wdl_tmlg','wdl_dth','wdl_cdth','wdl_dtl','wdl_cdtl','wdl_hbd','wdl_lbd','wdl_hbn','wdl_lbn','wdl_hrt','wdl_lrt','wdl_pts','wdl_ptl','wdl_cpts','wdl_cptl','wdl_trrev','wdl_psth','wdl_psdh','wdl_pstl','wdl_psdl','wdl_h3pat'])
    fls.extend(['wdl_l3pat','wdl_rths','wdl_lwcw','wdl_hwcw','wdl_tmgh','wdl_tmgl','wdl_ghdt','wdl_gldt','wdl_hbc','wdl_lbc','wdl_lhc','wdl_llc','wdl_hlc','wdl_gbhlr','wdl_thhc','wdl_tlhc','wdl_tllc','wdl_thlc','wdl_thhll','wdl_thr','wdl_trhh','wdl_trlh','wdl_trll','wdl_trhl','wdl_trud','wdl_pkhh'])
    fls.extend(['wdl_pklh','wdl_pkll','wdl_pkhl','wdl_pkud','wdl_lhlp','wvw_pkh','wvw_trh','wvw_wr','dly_hhcn5','dly_hhcn10','dly_hhcn20','dly_llcn5','dly_llcn10','dly_llcn20','dly_hhllcn5','dly_hhllcn10','dly_hhllcn20'])
    fls.extend(['dly_gap','dly_gapz','dly_wck1','dly_wck2','dly_wck3','dly_wck4','dly_wckav','dly_chl','dly_pos','dly_dnm','dly_csnm','dly_nmrt5','dly_nmrt10','dly_nmrt20','dly_dtwm'])
    fls.extend(['dly_cstwm','dly_twmrt5','dly_twmrt10','dly_twmrt20','dly_dftm','dly_dhdm','dly_hdif5','dly_ldif5','dly_dfrt5','dly_rge1','dly_rge2','dly_arg5','dly_arg10','dly_arg20','dly_lh3','dly_ls20r','dly_ls10r'])
    fls.extend(['rvs_d5','rvs_d1','tme_expt','tme_lexp','tme_cont','tme_wn','tme_dayow','tme_mno'])
    fls.extend(['rlp_syrzmx','dly_2dyrt','dly_5dyrt','dly_10dyrt','wdl_glrng'])
    fls.extend(['rlp_coravg5','rlp_coravg20','rlp_ysthlp','rlp_5dhlp','rlp_10dhlp','rlp_25dhlp','rlp_syr','rlp_syrz20'])
    
    if ticker in ['C','S','W','FC','LC','LH','SM','BO']:
        fls.extend ( ['ret_lwsd','wck_lwsd','tme_lwsd'])
        
    for i in range(len(rlpls)):
        fls.extend(['rlp_ret'+str(i), 'rlp_cor5_'+str(i),'rlp_cor20_'+str(i),'rlp_ysrdif'+str(i),'rlp_5drdif'+str(i),'rlp_20drdif'+str(i) ])
    fs={}
    for e in fls:
        fs[e]=[]
    return fs