import pymysql
import pandas as pd

class DBUpdater:
    def __init__(self):
        """
        MariaDB connect & code dict generate
        """
        self.conn = pymysql.connect(host='localhost', user='root', password='ksw09157', db='INVESTAR', charset='utf8')

        with self.conn.cursor() as curs:
            sql="""
            create table if not exists  companyInfo(
                code varchar(20),
                company varchar(40),
                lastUpdate date,
                primary key (code)
            )
            """
            curs.execute(sql)
            sql="""
            create table if not exists dailyPrice(
                code varchar(20),
                date date,
                open bigint(20),
                high bigint(20),
                low bigint(20),
                close bigint(20),
                diff bigint(20),
                volume bigint(20),
                primary key (code, date)
            )
            """
            curs.execute(sql)
        self.conn.commit()

        self.codes = dict()
        self.update_comp_info()

    def __del__(self):
        """
        disconnect MariaDB
        """
        self.conn.close()

    def read_krx_code(self):
        """
        from KRX read list change to dataframe
        """
        url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code','회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
    
    def update_comp_info(self):
        """
        update code in companyInfo table
        save dict
        with replace into
        """
        sql = "select * from companyInfo"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]]=df['company'].values[idx]
        with self.conn.cursor() as curs:
            sql = "select max(last_update) from company_info"

    def read_naver(self, code, company, pages_to_fetch):
        """
        read naver finance change to dataframe
        """
    
    def replace_into_db(self, df, num, code, company):
        """
        read naver finance replace into db
        """

    def update_daily_price(self, pages_to_fetch):
        """
        from naver read KRX update into db
        """
    
    def execute_daily(self):
        """
        run naverFinance or 5pm update daily_price table
        """

if __name__ = '__main__':
    dbu = DBUpdater()
    dbu.execute_daily