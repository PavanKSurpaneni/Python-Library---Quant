# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 12:09:01 2020

@author: Pavankumar
"""


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import talib,ta
import seaborn as sns
import  mplfinance as mpf
#%%
df=pd.read_csv('C:\\Users\\Pavankumar\\Desktop\\BITCOIN_DATA\\Gemini_BTCUSD_d.csv',skiprows=1)
df=df.iloc[::-1]
df['year']=[int(x[:4]) for x in list(df['Date']) ]
df['Date']=pd.to_datetime(df['Date'],format='%Y-%m-%d')
df.set_index('Date',inplace=True)

#plt.figure(figsize=(20,9))
#plt.plot(list(df['Volume USD']))
#plt.show()

df.drop('Symbol',axis=1,inplace=True)

df['1d_ret']=100*(df['Open']/df['Open'].shift(1)-1)
df['fret']=df['1d_ret'].shift(-1)

df=df.iloc[1:-1,:]

print (df.head(5))
print (df.columns)
#%% Let's explore the data for bitcoin
sns.distplot(df['1d_ret'],kde=False)
plt.title('Returns distribution of Bitcoin')
plt.show()

sns.boxplot(df['1d_ret'])
plt.title('Box plot of Bitcoin 1 day returns')
plt.show()

print ( f'Bitcoin is up on {round(100*df[df["1d_ret"]>0].shape[0]/df.shape[0],1)} % days and down on {round(100*df[df["1d_ret"]<0].shape[0]/df.shape[0],1)}% days')

# Let's see if this how consistent is up to down days distribution across years
print (' % of days Bitcoin was up - year wise')
df['updays']=np.where(df['1d_ret']>=0,1,0)
print ( round(100*df.groupby('year')['updays'].sum()/df.groupby('year')['updays'].count(),1) )

# Lets plot volatility of bitcoin
df['vol60']=np.sqrt(252)*df['1d_ret'].rolling(window=60).std()
df['vol250']=np.sqrt(252)*df['1d_ret'].rolling(window=250).std()

# Let's now plot the 60 day rolling vol
plt.figure(figsize=(15,9))
plt.plot(df['vol60'])
plt.plot(df.index.values,[df['vol60'].mean()]*df.shape[0])
plt.title('60 day rolling volatility of Bitcoin')
plt.show()

# Let's see how the candlestick chart for a bull year (2017) and a bear year (2018) : Using latest mplfinance library
for yr in [2017,2018]:
    #plt.figure(figsize=(14,9))
    mpf.plot(df[df['year']==yr][['Open','High','Low','Close']].iloc[:125],type='candle',mav=(9,50),title=f'Bitcoin Candlestick chart {yr} first half')
    #plt.title(f' Bitcoin price chart : {yr} : Big bull year')
    plt.show()
    
#%% Get plots for analysis
for i in range(0,df.shape[0],50):
    #plt.figure(figsize=(14,9))
    mpf.plot(df[['Open','High','Low','Close']].iloc[i:i+100],type='candle',figratio=(20,10),mav=(9,50),title=f'Bitcoin Candlestick chart {df.index.values[i]} - {df.index.values[i+100]}',savefig=f'C:\\Users\\Pavankumar\\Desktop\\bitcoin_plots\\{i}.png')
    #plt.title(f' Bitcoin price chart : {yr} : Big bull year')
    #plt.savefig()
    plt.show()    

#%%
# Let's implement a simple Momentum Strategy where we go long if last x days return is positive and short otherwise 
for mom_per in [5,20,50]:
    df[f'{mom_per}d_ret']=100*(df['Open']/df['Open'].shift(mom_per)-1)
    df[f'sig_mom{mom_per}']=np.sign(df[f'{mom_per}d_ret'])
    df[f'pnl_mom{mom_per}']=df[f'sig_mom{mom_per}'].values*df['fret'].values
    print (f'sharpe of {mom_per} day momentum strategy is ',np.sqrt(252)*df[f'pnl_mom{mom_per}'].mean()/df[f'pnl_mom{mom_per}'].std())

#%%
# Let's see the data separation caused by 5 dma
sns.distplot(df[df['5d_ret']>0]['fret'],color='g')    
sns.distplot(df[df['5d_ret']<0]['fret'],color='g')
#%% # Let's now implement a moving average strategy: 
# where if faster moving average above slower moving average we go long otherwise short 

for n_ma in [4,9,20,50,100]:
    df[f'ma_{n_ma}']=df['Close'].rolling(window=n_ma,min_periods=2).mean().shift(1)
 
for f,s in [(4,100),(9,20),(50,100)]:
    df[f'sig_{f}_{s}']=np.where(df[f'ma_{f}']>df[f'ma_{s}'],1,-1 )
    df[f'pnl_{f}_{s}']=df[f'sig_{f}_{s}'].values*df['fret'].values
    print (f'sharpe ma {f}_{s} is',round(np.sqrt(252)*df[f'pnl_{f}_{s}'].mean()/df[f'pnl_{f}_{s}'].std(),2) )
#%% # Let's now create a strategy based on MACD 
df['macd'], df['macdsig'], macdhist = talib.MACD(df['Close'], fastperiod=12, slowperiod=26, signalperiod=9)
df['macd'], df['macdsig']=df['macd'].shift(1), df['macdsig'].shift(1)
df['sig_macd']=[1 if df['macd'].iloc[i]>df['macdsig'].iloc[i] else -1  for i in range(df.shape[0]) ]
df['pnl_macd']=df['sig_macd']*df['fret']
print (np.sqrt(252)*df['pnl_macd'].mean()/df['pnl_macd'].std())

pnl_cols=[x for x in df.columns if x[:3]=='pnl']
df['tot_pnl']=df[pnl_cols].mean(axis=1)
print (np.sqrt(252)*df['tot_pnl'].mean()/df['tot_pnl'].std())

#%% Let's do some Portfolio Construction
mean_daily_returns=df[pnl_cols].mean()
cov_matrix = df[pnl_cols].cov()

n_sims=5000
results = np.zeros((3,n_sims))
best_weights=np.zeros(7)
max_shrp=0

for i in range(n_sims):
    #select random weights for portfolio holdings
    weights = np.random.random(7)
    #rebalance weights to sum to 1
    weights /= np.sum(weights)
    
    #calculate portfolio return and volatility
    portfolio_return = np.sum(mean_daily_returns * weights) * 252
    portfolio_std_dev = np.sqrt(np.dot(weights.T,np.dot(cov_matrix, weights))) * np.sqrt(252)
    
    #store results in results array
    results[0,i] = portfolio_return
    results[1,i] = portfolio_std_dev
    #store Sharpe Ratio (return / volatility) - risk free rate element excluded for simplicity
    results[2,i] = results[0,i] / results[1,i]
    
    if results[2,i]>max_shrp:
        best_weights=[round(x,2) for x in weights]
        max_shrp=results[2,i]
#convert results array to Pandas DataFrame
results_frame = pd.DataFrame(results.T,columns=['ret','stdev','sharpe'])

#create scatter plot coloured by Sharpe Ratio
plt.scatter(results_frame.stdev,results_frame.ret,c=results_frame.sharpe,cmap='RdYlBu')
plt.title('Efficient frontier ')
plt.colorbar()

print (' The max sharpe we can achieve with ideal weights is ',max_shrp)
print ( list(zip(pnl_cols,best_weights)))
#%%
df['strat1_sig_open']=np.where(df['1d_ret']>0,df['fret'],0)
print (np.sqrt(252)*df['strat1_sig_open'].mean()/df['strat1_sig_open'].std())

#%%
print (df.head(2))
print (df.tail(2) )
print (df.shape[0])

#%% Strategy based on S&P 500 
sp=pd.read_csv('C:\\Users\\Pavankumar\\Desktop\\sp500_yahoo.csv')
sp['Date']=[int( f'{x[:4]}{x[5:7]}{x[8:10]}') for x in list(sp['Date']) ]

sp['sp_9ma']=np.where(sp['Close']>=sp['Close'].shift(9),1,0)
sp['sp_20ma']=np.where(sp['Close']>=sp['Close'].shift(20),1,0)
sp['sp_50ma']=np.where(sp['Close']>=sp['Close'].shift(50),1,0)
sp['sp_100ma']=np.where(sp['Close']>=sp['Close'].shift(100),1,0)
#sp['sp_200ma']=np.where(sp['Close']>=sp['Close'].shift(100),1,0)
sp=sp[(sp['Date']>=20151009)&(sp['Date']<=20200416)]

print (sp[['Date','sp_9ma','sp_20ma','sp_50ma','sp_100ma']].head(10))
#sp=sp.iloc[100:,:]
sp['sp_ret']=100*(sp['Open']/sp['Open'].shift(1)-1)
sp['sp_ret3']=100*(sp['Open']/sp['Open'].shift(3)-1)
sp['sp_ret5']=100*(sp['Open']/sp['Open'].shift(5)-1)
sp['sp_ret10']=100*(sp['Open']/sp['Open'].shift(10)-1)
sp=sp.iloc[10:,:]

print (sp.isnull().sum())
sp['Date']=pd.to_datetime(sp['Date'],format='%Y%m%d')
sp.set_index('Date',inplace=True)
sp=sp.join(df['1d_ret'],how='left')
sp.rename(columns={'1d_ret':'btc_ret'},inplace=True)
sp['btc_fret']=sp['btc_ret'].shift(-1)
sp=sp.iloc[1:-1,:]
print (sp.head(10))
print (np.corrcoef(sp['btc_ret'],sp['sp_ret'])[0][1])
#%%
print (sp['btc_fret'].mean())
#%%
print ( sp[sp['sp_ret']>0]['btc_fret'].mean(),sp[sp['sp_ret']<0]['btc_fret'].mean() )
print ( sp[sp['sp_ret3']>0]['btc_fret'].mean(),sp[sp['sp_ret3']<0]['btc_fret'].mean() )
print ( sp[sp['sp_ret5']>0]['btc_fret'].mean(),sp[sp['sp_ret5']<0]['btc_fret'].mean() )
print ( sp[sp['sp_ret10']>0]['btc_fret'].mean(),sp[sp['sp_ret10']<0]['btc_fret'].mean() )

#%%
print ( np.sqrt(252)*sp[sp['sp_50ma']==0]['btc_fret'].mean()/sp[sp['sp_50ma']==0]['btc_fret'].std() )
#%%
print ( sp[sp['sp_9ma']>0]['btc_fret'].mean(),sp[sp['sp_9ma']==0]['btc_fret'].mean() )
print ( sp[sp['sp_20ma']>0]['btc_fret'].mean(),sp[sp['sp_20ma']==0]['btc_fret'].mean() )
print ( sp[sp['sp_50ma']>0]['btc_fret'].mean(),sp[sp['sp_50ma']==0]['btc_fret'].mean() )
print ( sp[sp['sp_100ma']>0]['btc_fret'].mean(),sp[sp['sp_100ma']==0]['btc_fret'].mean() )


