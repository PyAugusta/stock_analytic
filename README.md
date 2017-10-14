# Python Stock Market Analysis

## The process
1. Get historical data
    - Stock market open/close
    - Archival finance news (i.e. New York Times finance section)
2. Create mechanisms to gather current data on a daily basis
3. Perform sentiment analysis against news data to determine positive v. negative coverage.
    - To keep it simple for now (and avoid the need for manual classification and training), 
    I will create one list of positive terms, and one for negative terms.
    - If more positive terms exist between open and close, the days news will be considered
    positive, and vice versa.
4. Determine if there is a correlation between the sentiment of the news stories and the 
market's behaviour.
5. Use the data collected to train/test a machine learning algorithm.
6. Predict future stock market behaviour based on the sentiment analysis of the previous days news.
