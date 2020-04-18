# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 11:27:08 2020

@author: Pavankumar
"""


#%%  This function gets the roll file and the wasde dates
def create_roll_wsd(fs,datespath,sypath,ticker,syms,iss):
    vp=pd.read_csv(datespath+syms[ticker]+'.csv')
    all_dates=list(vp['date'].unique())     
    pdts=pd.read_csv(sypath,sep='\t',skiprows=1)
    #print (pdts.head(5))
    sydates=[int(x) for x in list(pdts['Date'].unique() ) ]

    rols=pd.read_csv(rolspath+ticker.lower()+'.rolls',delimiter='\t',usecols=[0,1,2],header='infer',names=['cont','contyr','expdate'])
    ldate=rols['expdate'].iloc[len(rols)-2] 
    sydates=[ x for x in sydates if (x>=iss and x<=ldate) ]
    
    op=f'{featfls_path}{ticker}/{ticker}_feats.csv'
    sti=0
    if os.path.isfile(op):
        curfile=pd.read_csv(op)  
        cdate=curfile['dates'].iloc[-1]
        sti=bisect_right(sydates,cdate)

        if sti==len(sydates):
            return None,None,None,None,None,None,sydates,None,sti
        for e in fs:
            fs[e]=list(curfile[e])
            
        fs['dates']=[ pd.to_datetime(int(x),format='%Y%m%d').date() for x in list(fs['dates']) ]
        
    j=0
    while (j<=rols.shape[0]-1 ) and (rols['expdate'].iloc[j]<sydates[sti]):
        j+=1
    
    rid=j
    nln=len(ticker)+1
    yri=[len(str(x[nln:])) for x in list(rols['cont'] )]
    mnl=min(yri)
    
    if mnl==1:
        rols['cont']=[ rols['cont'].iloc[i][:nln] + str(rols['expdate'].iloc[i])[2:4] if rols['cont'].iloc[i][-1]==str(rols['expdate'].iloc[i])[3] else rols['cont'].iloc[i][:nln]+ str(int(str(rols['expdate'].iloc[i])[:4])+1)[2:4] for i in range(rols.shape[0]) ] 
    
    rols['cont']=[syms[ticker]+x[-3:] for x in list(rols['cont'])]
    exp=int(rols['expdate'].iloc[rid])
    cont=rols['cont'].iloc[rid]
        
    wsd=pd.read_csv(dirpath+'release_dates.csv',header='infer',names=['dates'])
    wsd['dates']=[x.date() for x in pd.to_datetime(wsd['dates'],format='%Y%m%d') ]
    wdates=list(wsd['dates'])
    
    return fs,rols,exp,cont,rid,wdates,sydates,all_dates,sti         