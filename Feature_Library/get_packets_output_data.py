# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:29:19 2020

@author: Pavankumar
"""


#%% Do the corrections in naming
# normalize for features the right way (some divided by price & vol )
# shift the y by -1 so we predict the next days return i.e at this open predict this open to next open return
# output the data
def output_data(fs,rlpls,ticker,sti):    
    feats=[x for x in list(fs.keys()) ]  # ['ylg','opn','close','hi','lo'] drop these 5 columns while training for backtests
    mdata=pd.DataFrame(columns=feats) 
    
    for f in feats: 
        mdata[f]=np.array(fs[f])
  
    mdata['vol20']= mdata['y'].rolling(20).std().values
    mdata['wdl_ty']=[ x if x in [1,-1] else 1 if x=='hi' else -1 for x in fs['wdl_ty'] ]
    cont_map={'F':0,'G':1,'H':2,'J':3,'K':4,'M':5,'N':6,'Q':7,'U':8,'V':9,'X':10,'Z':11}    
    mdata['tme_cont']= [ x if x in list(range(12)) else cont_map[x.upper()] for x in fs['tme_cont'] ]    
    mdata['dates']=[ int(str(x)[:4]+str(x)[5:7]+str(x)[8:10]) for x in list(mdata['dates'].values)]
    mdata.to_csv(f'{featfls_path}{ticker}/{ticker}_feats.csv',float_format='%.12f',index=False)
    
#%% Used to generate packets of symbol, related products, pit time, start date for all symbols and created a list of lists
def get_packets(dirpath):
    ans=[]
    sydf=pd.read_excel(dirpath+'sym_info.xlsx',sheetname='ds001_universe')
    
    symp=get_mapping(dirpath,'tickdata')
    
    for e in range(sydf.shape[0]):
        if sydf['name'].iloc[e] in ['W','C','S']:
            tmp=[sydf['name'].iloc[e]]
            tmp.extend( [symp[x] for x in list(sydf['related_feasible'].iloc[e].split(',')) ] )
            if len(str(sydf[pit_time].iloc[e]))==3:
                tmp.append(str('0'+str(sydf[pit_time].iloc[e]) ) )
            else:
                tmp.append(str(sydf[pit_time].iloc[e]) )
            tmp.append( max( sydf['Product-START'].iloc[e],sydf['RLP-START'].iloc[e],19950101 ) )
            ans.append( tmp)

    return ans