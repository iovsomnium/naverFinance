import matplotlib.pyplot as plt
from Investar import Analyzer #1

mk = Analyzer.MarketDB()
df = mk.get_daily_price('005930', '2017-07-10')

plt.figure(figsize=(9,6))
plt.subplot(2, 1, 1)
plt.title('Samsung Electronics (Investar Data)')
plt.plot(df.index, df['close'], 'c', label='Close') # 4
plt.legend(loc='best')
plt.subplot(2,1,2)
plt.bar(df.index, df['volume'], color='g', label='Volume')
plt.legend(loc='best')
plt.show()