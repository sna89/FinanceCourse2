import yfinance as yf

start_date = "2009-01-01"
end_date = "2019-12-31"

for stock in ["AAPL", "SBUX", "MSFT", "CSCO", "QCOM"]:
    data = yf.download(stock, start=start_date, end=end_date)
    data.to_csv('{stock}.csv'.format(stock=stock))

