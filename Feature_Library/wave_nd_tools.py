# -*- coding: utf-8 -*-
"""
@author: Pavankumar
"""

# This file has wave related functions and other help functions
#%% Check if a symbol and it's related products' price file exists, if not this creates them
# This function uses gen_price function from pricefiles_frm_mbars.py file (which internally checks if the file exists at the path, if not it creates)
def check_pf(ls,syms,optime,dirpath):
    sydf=pd.read_excel(dirpath+'sym_info.xlsx',sheetname='ds001_universe')
    stdt_mp={ sydf['name'].iloc[i]:max(sydf['Product-START'].iloc[i],19950102) for i in range(sydf.shape[0]) }
    
    for e in ls:
        try:
            gen_price( [syms[e],optime,stdt_mp[e] ] )
        except:
            x=min(sydf[sydf['related_feasible'].str.contains(syms[e])]['RLP-START'])  # for symbols we aren't tradign but need as related product (for ex: URO) -> take all rows where URO is a related product, take the min. of all those symbols rlp-start date
            gen_price( [syms[e],optime,x ] )
            
    return ls
#%% This take a daily ohlc dataframe, check for wave rules to flag highs nd lows: it takes 2 days for a low / high to be included , coz. it see open-open of today and open-open of tomorrow to confirm, the extrema holds for the next day also
# The correct column says whether a detected extrema was correct on not by seeing if it held for next 2 days (for ex: a low was detected nd not breached for the next 2 days, it will be correct )
def wave_daily(dat):
    hldat=pd.DataFrame(columns=['indx','date','hilo','prc','open','high','low','close','start','know','correct','op1','hi1','lo1','cl1'])
    hlin=0
    doo=False
    loo=False
    hf='no'
    
    dshp=dat.shape[0]
    
    for i in range(dshp-2):
        ch=float(dat['high'].iloc[i])
        cl=float(dat['low'].iloc[i])
        co=float(dat['open'].iloc[i])
        cc=float(dat['close'].iloc[i])
        
        tm=dat.index[i]
        co1=float(dat['open'].iloc[i+1])
        ch1=float(dat['high'].iloc[i+1])
        cl1=float(dat['low'].iloc[i+1])
        cc1=float(dat['close'].iloc[i+1])
        
        if i==dshp-3:
            knw=dat.index[i+2] + timedelta(days=1)
        else:
            knw=dat.index[i+3]
            
        if (i>2):
            if (((dat['high'].iloc[i-3]-dat['low'].iloc[i-3])/(dat['close'].iloc[i-3]) )<-1.5) and ((ch>max(dat['high'].iloc[i-2],dat['high'].iloc[i-1])) and (ch>=dat['high'].iloc[i+1])):
                doo=True   
                
            if ( ((dat['high'].iloc[i-3]-dat['low'].iloc[i-3])/(dat['close'].iloc[i-3]) )>-1.5) and ((ch>max(dat['high'].iloc[i-3],dat['high'].iloc[i-2],dat['high'].iloc[i-1])) and (ch>=dat['high'].iloc[i+1])):
                doo=True
            
        elif (i==2) and ( (ch>max(dat['high'].iloc[i-2],dat['high'].iloc[i-1])) and (ch>=dat['high'].iloc[i+1]) ):
            doo=True
        elif (i==1) and ( (ch>dat['high'].iloc[i-1]) and (ch>=dat['high'].iloc[i+1]) ):
            doo=True
        elif (i==0) and (ch>dat['high'].iloc[i+1]):
            doo=True
        else:
            pass

        while doo:
            
            if ch>=dat['high'].iloc[i+2]:
                cor='yes'
            else:
                cor='no'
            
            
            hldat.loc[hlin]= [i,tm,'hi',ch,co,ch,cl,cc,dat.index[i+2],knw,cor,co1,ch1,cl1,cc1] 
            hlin+=1  
            hf='yes'
            doo=False
            
        if hf!='yes':
            if (i>2):
                if (((dat['high'].iloc[i-3]-dat['low'].iloc[i-3])/(dat['close'].iloc[i-3]) )>1.5) and ((cl<min(dat['low'].iloc[i-2],dat['low'].iloc[i-1])) and (cl<=dat['low'].iloc[i+1])):
                    loo=True
                     
                if (((dat['high'].iloc[i-3]-dat['low'].iloc[i-3])/(dat['close'].iloc[i-3]) )<1.5) and ((cl<min(dat['low'].iloc[i-3],dat['low'].iloc[i-2],dat['low'].iloc[i-1])) and (cl<=dat['low'].iloc[i+1])):
                    loo=True
                    
            elif (i==2) and ((cl<min(dat['low'].iloc[i-2],dat['low'].iloc[i-1])) and (cl<=dat['low'].iloc[i+1])):
                loo=True
            elif (i==1) and ((cl<dat['low'].iloc[i-1]) and (cl<=dat['low'].iloc[i+1])):
                loo=True
            elif (i==0) and (cl<=dat['low'].iloc[i+1]):
                loo=True
            else:
                pass
            
            while loo:
                if cl<=dat['low'].iloc[i+2]:
                    cor='yes'
                else:
                    cor='no'
                
                hldat.loc[hlin]= [i,tm,'lo',cl,co,ch,cl,cc,dat.index[i+2],knw,cor,co1,ch1,cl1,cc1] 
                hlin+=1  
                hf='yes'
                doo=False
                loo=False
            
        hf='no'

    return hldat  
#%% Takes a contract for ex: SYH18 a start nd end date, then uses get_cont_ohlc function (from the other python file)  which :  gets all minute bars for that contract between those dates, creates a ohlc daily from those minute bars using optm as opentime
# Note: The start date will be about 120 days before this contract became first, so it has all the history to see highs and lows
def data4_wave(ct,st,exp,td_ticker,sydates,all_dates,optm):
    #print ('here ' , ct , st , exp2)
    dd=get_cont_ohlc(ct,st,exp,td_ticker,sydates,all_dates,optime=optm)
    dd.rename(index=str,columns={'Date':'date','Open':'open','High':'high','Low':'low','Close':'close'},inplace=True)
    dd['date']=[x.date() for x in list(dd['date'])]
    dd.set_index('date',inplace=True)
    
    xtr=wave_daily(dd)
    
    return xtr,dd

#%% A helper function to count successive counts of a number from 1st index, stops at the first occurence of a number in list l other than num (Default :1 )
def cnt_ones(l,num=1):
    
    ln=len(l)
    cnt=0
    for i in range(ln):
        if l[i]!=num:
            break
        else:
            cnt+=1
    return cnt
#%% Returns a non-zero number when a number is 0, used for denominators in fractions to avoid infs
def nzero(r):
    if r ==0:
        return 0.001
    else:
        return r


def ls3_cnt(ls):  # count all positive numbers in a list
    cnt=0
    for e in ls:
        if e>0:
            cnt+=1
    return cnt    