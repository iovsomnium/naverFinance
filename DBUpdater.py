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
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today: # 가장 최근 업데이트 날짜를 가져올대 존재하지 않거나 오늘보다 오래된 경우 업데이트한다.
                krx = self.read_krx_code() # 상장 기업 목록 파일을 읽어서 krx 데이터 프레임에 저장한다.
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"replace into companyInfo (code, company, lastUpdate) values ('{code}','{company}','{today}')"
                    curs.execute(sql) # replace into 구문을 통해 종목코드, 회사명, 오늘 날짜' 행을 DB에 저장한다.
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] {idx:04d} replace into companyInfo values ({code}, {company}, {today})")
                self.conn.commit()
                print('')

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