import pandas as pd
from pandas_datareader import data
import datetime as dt
from datetime import timedelta, date
import matplotlib.pyplot as plt
from scipy import stats
import sqlite3 as sl


def runTradeAlgo():    
    conn = sl.connect("AlgoDB.db")
    #cursor = conn.cursor()

    print("Running Script... Be sure to run after market close and stock data is posted to Yahoo Finance for the day.") 
    #Variables
    dateRangeVar = 180
    slopeThresh = .1
    varThresh = .05
    peakThresh = 10
    troughThresh = 10
    buyAmt = 100
    sellAmt = 100
    startingMoney = 10000

    #Set this to 0 for automatic calculation, Recommended be set 20 - 25:
    manualVixThresh = 22

    #(yes/no) Calculate Slope and Var thresh?
    slopeVarCalc = 'yes'

    #Create Clean Reset DF
    statsDFBlank = pd.DataFrame(columns = ['Symbol', 'LastPrice', 'Days', 'StDev', 'Avg', 'Slope', 'Std/Avg', '#ofPeaks', '#ofTroughs'])
    statsDF = statsDFBlank

    #set date range
    endDate = date.today()
    dateRange = dt.timedelta(dateRangeVar)
    startDate = endDate - dateRange

    #check if should run
    try:
        lastHoldingsTest = pd.read_sql_query("select * from HoldingsHistory", conn)
        lastHoldingsTest = pd.to_datetime(lastHoldingsTest['Date'].max())
        if lastHoldingsTest == None:
            print('Error finding max date... will Stop.')
            canRun = False
        elif endDate > lastHoldingsTest:
            print('Script has not run previously Today... Continuing.')
            canRun = True
        else:          
            print('Data already exists for today...')
            canRun = False
            #canRun = True  
    except:
        print('No Database Detected, a new one will be created... Continuing.')
        canRun = True

    #Second check if should run    
    MKTTest = data.DataReader(['^GSPC'], 'yahoo', start=startDate, end=endDate).reset_index()
    MKTTest = MKTTest['Date'].max()
    if MKTTest.date() == endDate:
        print("Market was open today, Continuing...")
        canRun2 = True
    else:
        print("No new Stock Data, was the market open today?")
        canRun2 = False
        #canRun2 = True

    if canRun == True and canRun2 == True:  # Can also use this: date.today().weekday() <=4 
        print("Pulling Stock Data") 
        
        #Pull all S&P stocks
        wiki = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        w = wiki[0]
        wp = w[['Symbol','GICS Sector']]
        w = w['Symbol']        
        w = w.reset_index()
        #w = w.loc[:2]
        w = w.append({'Symbol':'^VIX'}, ignore_index = True)
        w = w.append({'Symbol':'^GSPC'}, ignore_index = True)
        iterations = len(w)

        #Generate Export DF
        print("Calculating Stock Stats") 
        for x in range(iterations): 
            try:
                #Pick a ticker
                myTicker = w['Symbol'].iloc[x]

                #List Prices
                Prices = data.DataReader([myTicker], 'yahoo', start=startDate, end=endDate)         
                Prices = Prices['Adj Close']
                Prices = Prices.reset_index()

                #Find Stats
                stdPrice = Prices.std()[myTicker]
                avgPrice = Prices.mean()[myTicker]
                tradeDays = Prices.count()[myTicker]

                #Total Peaks and Troughs
                Prices['Peak'] = Prices[myTicker].apply(lambda x: 1 if x >= (avgPrice + stdPrice)  else 0)
                Prices['Trough'] = Prices[myTicker].apply(lambda x: 1 if x <= (avgPrice - stdPrice)  else 0)

                #Assign Peaks and Troughs
                totalPeaks = Prices.sum()['Peak']
                totalTroughs = Prices.sum()['Trough']

                #Find slope
                slope, intercept, r, p, se = stats.linregress(Prices.index, Prices[myTicker])

                #Select most recent price
                mostrecent = Prices[myTicker].iloc[len(Prices)-1]

                #How volitaile
                percentStd = stdPrice / avgPrice

                #Add Export Data
                exData = {'DataDate' : endDate,
                    'Symbol': myTicker,
                    'LastPrice': mostrecent,
                    'Days': tradeDays,
                    'StDev': stdPrice,
                    'Avg': avgPrice,
                    'Slope': slope,
                    'Std/Avg': percentStd,
                    '#ofPeaks': totalPeaks,
                    '#ofTroughs': totalTroughs}
                statsDF = statsDF.append(exData, ignore_index = True)
            except:
                continue   

        #generate describe df
        statsStatsDF = statsDF.describe()

        #functions to set auto-limits
        if slopeVarCalc == 'yes':
            varThresh = statsStatsDF['Std/Avg'].iloc[4]
            slopeThresh = abs(statsStatsDF['Slope'].iloc[5])
            peakThresh = statsStatsDF['#ofPeaks'].iloc[5]
            troughThresh = statsStatsDF['#ofTroughs'].iloc[5]

        #apply Buy and Sell Price and Logic
        statsDFTwo = statsDF
        statsDFTwo['buyPrice'] = statsDFTwo['Avg'] - statsDFTwo['StDev']
        statsDFTwo['sellPrice'] = statsDFTwo['Avg'] + statsDFTwo['StDev']
        statsDFTwo.loc[statsDFTwo['LastPrice'] <= statsDFTwo['buyPrice'], 'Buy?'] = 'yes' 
        statsDFTwo.loc[statsDFTwo['LastPrice'] >= statsDFTwo['sellPrice'], 'Sell?'] = 'yes'

        statsDFTwo=statsDFTwo.fillna('no')

        def f(row):
            if row['Buy?'] == 'yes' or row['Sell?'] == 'yes':
                val = 'yes'
            else:
                val = 'no'
            return val

        statsDFTwo['Buy or Sell?'] = statsDFTwo.apply(f, axis=1)

        def s(row):
            if abs(float(row['Slope'])) <= slopeThresh and row['Std/Avg'] >= varThresh and row['#ofPeaks'] >= peakThresh and row['#ofTroughs'] >= troughThresh:
                val = 'yes'
            else:
                val = 'no'
            return val
        statsDFTwo['Qualifying Stock?'] = statsDFTwo.apply(s, axis=1)

        #filter  for only reccomended buys and sells
        statsDFThree = statsDFTwo[statsDFTwo['Qualifying Stock?'] == 'yes']
        statsDFThree = statsDFThree[~(statsDFThree['Symbol'].isin(['^VIX','^GSPC']))]

        #------BEGIN CODE FOR TRADING-----

        #Function to reset Owned Stocks
        global newBal
        global stockLedger

        try:
            stockLedger = pd.read_sql_query("select * from stockLedger", conn)
            AccValTrend = pd.read_sql_query("select * from AccValTrend", conn)
            aggHoldings = pd.read_sql_query("select * from HoldingsHistory where [Date] = (Select MAX([Date]) from HoldingsHistory)", conn)
            newBal = aggHoldings[aggHoldings['Symbol'] == 'Cash']['value']
            newBal = int(newBal)     
            print("Pulled from DB")   
        except:
            stockLedger = pd.DataFrame(columns = ['Date', 'Buy/Sell', 'Symbol', 'Price', 'Shares', 'Amount', 'cashBalance'])
            AccValTrend = pd.DataFrame(columns = ['Date', 'Total Account Value'])
            startingBalance = startingMoney            
            newBal = startingBalance
            print("Creating new DB") 

        #Function to place a trade
        def placeTrade(action, amount, ticker):            
            global newBal
            global stockLedger

            sharePrice = statsDFTwo[statsDFTwo['Symbol']==ticker]['LastPrice']
            sharePrice = float(round(sharePrice, 2)) ##changed from int()

            Date = endDate
            print(action + " " + str(ticker) + " for " + str(amount))
        
            if action == 'buy':
                newBal = newBal - amount  
                shareQuantity = amount / sharePrice
            elif action == 'sell':
                newBal = newBal + amount 
                shareQuantity = amount / sharePrice *-1
            
            trade = {'Date': Date,
                'Buy/Sell': action,
                'Symbol': ticker,
                'Price': sharePrice,
                'Shares': shareQuantity,
                'Amount': amount,
                'cashBalance': newBal}

            stockLedger = stockLedger.append(trade, ignore_index = True)
            return stockLedger

        #Place trades here:
        statsVIXCheck = statsDFTwo[statsDFTwo['Symbol'] == '^VIX'] 
        if manualVixThresh == 0:
            VixThresh = (statsVIXCheck['StDev'].iloc[0] + statsVIXCheck['Avg'].iloc[0])
        else:
            VixThresh = manualVixThresh

        if statsVIXCheck['LastPrice'].iloc[0] < VixThresh:
            print('VIX is at ' + str(statsVIXCheck['LastPrice'].iloc[0]) + ' and the Threshold is ' + str(VixThresh) + ' placing Trades...')
            loopsDF = statsDFThree['Symbol']
            loopsDF = loopsDF.reset_index()
            loops = len(loopsDF)
            for y in range(loops): 
                try:
                    #Pick a ticker
                    tradeTicker = statsDFThree['Symbol'].iloc[y]
                    buyIndicator = statsDFThree['Buy?'].iloc[y]
                    sellIndicator = statsDFThree['Sell?'].iloc[y]                    
                    if buyIndicator == 'yes':
                        placeTrade('buy', buyAmt, tradeTicker) 
                    elif sellIndicator == 'yes':
                        placeTrade('sell', sellAmt, tradeTicker) 
                    else:
                        continue
                except:
                    continue
        else:
            print('VIX is too high at ' + str(statsVIXCheck['LastPrice'].iloc[0]) + ' over the ' + str(VixThresh) + ' threshold. No trades will be placed today')

        print("Generating Holdings")
        #Compile Current Holdings
        stockLedger['Date'] = pd.to_datetime(stockLedger['Date'])
        aggHoldings = stockLedger.groupby(['Symbol']).agg({'Shares':sum,'Date':max})
        aggHoldings['Date'] = aggHoldings['Date'].astype(str).str[:10]
        stockLedger['Date'] = stockLedger['Date'].astype(str).str[:10]
        aggHoldings = aggHoldings.reset_index()
        aggHoldings = pd.merge(aggHoldings, statsDFTwo, how="left",left_on='Symbol',right_on='Symbol')
        #aggHoldings.to_sql("HoldingsHistoryTest", conn, index=False, if_exists='append')
        #aggHoldings = aggHoldings[['Symbol','Shares','Date','LastPrice']] might need?
        aggHoldings['value'] = aggHoldings['Shares'] * aggHoldings['LastPrice']

        #Adding in cost Basis
        def negativeCon(inp,chk):
            if chk < 0:
                inp = inp *-1
            return inp

        tempJoin = stockLedger[['Symbol','Shares','Amount']]
        tempJoin['tempSymbol'] = tempJoin['Symbol']
        tempJoin['CostBasis'] = tempJoin['Amount']
        tempJoin['CostBasis'] = tempJoin[['Amount','Shares']].apply(lambda x: negativeCon(*x), axis=1)
        tempJoin = tempJoin.groupby(['tempSymbol']).agg({'CostBasis':sum}).reset_index() 
        aggHoldings = pd.merge(aggHoldings, tempJoin, how="left",left_on='Symbol',right_on='tempSymbol')
        aggHoldings = aggHoldings[['Symbol','Shares','Date','LastPrice','value','CostBasis']]

        #Add Cash Reccord
        cashReccord = {'Symbol': 'Cash',
            'Shares': newBal,
            'Date': endDate,
            'LastPrice': 1,
            'value': newBal}
        aggHoldings = aggHoldings.append(cashReccord, ignore_index = True)
        
        #Trend Total Value
        totalValue = aggHoldings['value'].sum()
        TotalReccord = {'Date': endDate,
            'Total Account Value': totalValue}
        AccValTrend = AccValTrend.append(TotalReccord, ignore_index = True)
        
        #Adjust Holdings Date for Trending
        aggHoldings['Date'] = str(endDate)

        print("Exporting to DB")
        #Save to Database
        statsDFTwo.to_sql("StockHistory", conn, index=False, if_exists='append')
        aggHoldings.to_sql("HoldingsHistory", conn, index=False, if_exists='append')
        aggHoldings.to_sql("LastHoldings", conn, index=False, if_exists='replace')
        stockLedger.to_sql("stockLedger", conn, index=False, if_exists='replace')   
        AccValTrend.to_sql("AccValTrend", conn, index=False, if_exists='replace')
        statsStatsDF.to_sql("StatsTable", conn, index=False, if_exists='replace')
        wp.to_sql("Sectors", conn, index=False, if_exists='replace')

        #for Tableau
        print("Exporting to csv for Tableau")
        holdingHistory = pd.read_sql_query("select * from HoldingsHistory", conn)
        stockHistory = pd.read_sql_query("select * from StockHistory", conn)

        stockHistory.to_csv('StockHistory.csv', index=False)
        holdingHistory.to_csv('HoldingsHistory.csv', index=False)        
        stockLedger.to_csv('stockLedger.csv', index=False)       
        wp.to_csv('Sectors.csv', index=False)       

        print("Done")
    else:
        print('No need to run, Ending...')

if __name__ == '__main__':
    runTradeAlgo()

