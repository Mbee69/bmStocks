import datetime           # for start and end periods
import time                
import pandas as pd   # to return a data frame with the stock data
from kivy.lang import Builder
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.screenmanager import ScreenManager,Screen
from kivymd.app import MDApp
from kivymd.uix.tab import MDTabsBase
from kivymd.uix.picker import MDDatePicker
from kivymd.uix.list import TwoLineAvatarIconListItem
from kivy.uix.button import Button
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.dropdown import DropDown
import sqlite3
from sqlalchemy import create_engine 
from datetime import date
from os import path
import shutil

#from pandasgui import show

kvfile=Builder.load_file('./kivyStocks.kv')

class MainWindow(Screen):
    pass

class StockDetails(Screen):
    pass

class DivDetails(Screen):
    pass

class ScreenManagement(ScreenManager):
    pass

class Tab(FloatLayout, MDTabsBase):
    '''Class implementign content for a tab'''

class MYConfig:
    def __init__(self):
        self.db_name = "StockDB.db"
        self.csv_path = "/mnt/4TB/Daten/Benny/nextcloud/05_Medien/01_Finanzen/"
        self.base_url_div = "https://www.borsaitaliana.it/borsa/etf/dividendi.html?isin={ticker}&lang=it"

    def get_db_name(self):
        return self.db_name

    def get_base_url_div(self):
        return self.base_url_div

    def set_csv_path(self, csv_path):
        self.csv_path = csv_path
    
    def get_csv_path(self):
        return self.csv_path


class DBConnection:
    instance = None

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(DBConnection)
            return cls.instance
        return cls.instance

    def __init__(self, db_name='you-db-name'):
        self.name = db_name
        # connect takes url, dbname, user-id, password
        self.conn = self.connect(db_name)
        self.cursor = self.conn.cursor()

    def connect(self):
        try:
            return sqlite3.connect(self.name)
        except sqlite3.Error as e:
            pass

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    # write your function here for CRUD operations


class YahooAPI(object):
    def __init__(self, interval="1d"):
        self.base_url = "https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start_time}&period2={end_time}&interval={interval}&events=history"
        self.interval = interval
    
    def __build_url(self, ticker, start_date, end_date):
        return self.base_url.format(ticker=ticker, start_time=start_date, end_time=end_date, interval=self.interval)
    
    def get_ticker_data(self, ticker, start_date, end_date):
        # must pass datetime into this function
        epoch_start = int(time.mktime(start_date.timetuple()))
        epoch_end = int(time.mktime(end_date.timetuple()))
        df = pd.read_csv(self.__build_url(ticker, epoch_start, epoch_end))
        df.insert(1,"Ticker",ticker)
        df.insert(1,"ISIN","")
        df.insert(9,"Valuta","EUR")
        return df

    def save(self):
        pass

    def load(self):
        pass

class BorsaItaliana():
    def __init__(self):
        self.my_config = MYConfig()
        self.base_url_div = self.my_config.get_base_url_div()
        self.dividends = []
        print(self.load())
        
    def __build_url(self, ticker):
        return self.base_url_div.format(ticker=ticker)

    def get_new_ticker_div(self,ticker):
        print(self.__build_url(ticker))
        try:
            dfs = pd.read_html(self.__build_url(ticker), thousands='.', decimal=',')
            df = dfs[0]
            df.insert(1, "ISIN",ticker)
            df.columns = ['DataDividendo','ISIN','Provento','Valuta','DataPagamento','TipoPagamento']

            new_dividends = df.merge(self.dividends, indicator=True, how='outer')
            new_df=new_dividends[new_dividends['_merge'] == 'left_only']

            #del new_df["index"]
            del new_df["_merge"]
            
            #self.dividends.append(new_df, ignore_index=True)
            if not self.save(new_df):
                print("ERROR - DB SAVING")
            self.load()
            return new_df
        except:
            print("Url konnte nicht aufgerufen werden") 
            return pd.DataFrame

    def get_all_dividends(self):
        return self.dividends

    def save_to_csv(self, path):
        self.dividends.to_csv(path)

    def count(self):
        try:
            return self.dividends.count()[0]
        except:
            return 0

    def save(self,df):
        try:
            conn = sqlite3.connect(self.my_config.get_db_name())   
            df.to_sql("Dividends",conn,if_exists="append")
            return True
        except:
            return False

    def load(self):
        # SQLAlchemy connectable 
        try:
            print(self.my_config.get_db_name())
            cnx = create_engine('sqlite:///'+self.my_config.get_db_name()).connect() 
            self.dividends = pd.read_sql("Dividends",cnx)
            del self.dividends["index"]

            self.dividends['DataPagamento'] = pd.to_datetime(self.dividends['DataPagamento'], format='%d/%m/%y')
            self.dividends.sort_values(by='DataPagamento', ascending=False)
            #self.dividends.sort(['DataPagamento'])
        except:
            print("ERROR - Load")
        return self.dividends


class myStocks():
    def __init__(self):
        # Start Stocks
        self.stocks = [["Pf", "DE0002635265","EXHE.F"],
        ["HY", "IE00B66F4759","ISHHF"],
        ["gHY", "IE00B74DQ490",""],
        ["EMLocal", "IE00B5M4WH52",""],
        ["EM", "IE00B9M6RS56",""],
        ["Stoxx600", "DE0002635307",""],
        ["Div30", "DE0002635299",""],
        ["EMDiv", "IE00B6YX5B26",""],
        ["EuroDiv", "IE00B0M62S72",""],
        ["Property", "IE00B1FZS350",""],
        ["Agg", "IE00B3DKXQ41",""],
        ["gDiv", "DE000A0F5UH1",""]]
        self.current_stock_isin = "DE0002635265"

        # for stock in self.stocks:
        #     print(stock[0])
        #     self.insert_db(stock[0],stock[1],"")


    def insert_db(self, name, isin, ticker):
        conn = sqlite3.connect(self.my_config.get_db_name())
        c = conn.cursor()

        #Tabelle für Stocks anlegen
        c.execute('INSERT INTO STOCKS (Name, ISIN, Ticker) VALUES ("'+name+'","'+isin+'","'+ticker+'");')
        conn.commit()

    def set_current_stock_isin(self,current_stock_isin):
        self.current_stock_isin = current_stock_isin

    def get_current_stock_isin(self):
        return self.current_stock_isin

    def get_current_stock_ticker(self):
        return self.get_ticker_from_isin(self.current_stock_isin)

    def get_current_stock_name(self):
        return self.get_name_from_isin(self.current_stock_isin)

    def get_stocks(self):
        return self.stocks

    def get_name_from_isin(self,stock_isin):
        for stock in self.stocks:
            if stock[1] == stock_isin:
                return stock[0]

    def get_isin_from_name(self,stock_name):
        for stock in self.stocks:
            if stock[0] == stock_name:
                return stock[1]

    def get_ticker_from_isin(self,stock_isin):
        for stock in self.stocks:
            if stock[1] == stock_isin:
                return stock[2]


class App(MDApp): 

    def build(self): 
        A = ScreenManagement()
        A.current = 'main'

        #conn = sqlite3.connect(MYConfig().get_db_name())
        #c = conn.cursor()

        #c.close()
        #conn.close()

        # #Tabelle für Stocks anlegen
        #c.execute('CREATE TABLE IF NOT EXISTS STOCKS (Name, ISIN, Ticker)')
        #conn.commit()

        # #Tabelle für History anlegen
        # c.execute('CREATE TABLE IF NOT EXISTS Historie (Date, ISIN, Ticker, Open, High, Low, Close, AdjClose, Volume, Valuta)')
        # conn.commit()

        # #Tabelle für Div anlegen
        # c.execute('CREATE TABLE IF NOT EXISTS Dividends (DataDividendo, ISIN, Provento, Valuta, DataPagamento, TipoPagamento)')
        # conn.commit()

        return A

    def on_start(self):
        self.my_config = MYConfig()
        self.my_stocks = myStocks()
        self.borsaitaliana = BorsaItaliana()
        self.yahoo = YahooAPI()
        self.init_stocks_ist()

        # Backup Database
        if path.exists(self.my_config.get_db_name() ):
            shutil.copyfile(self.my_config.get_db_name(), self.my_config.get_db_name().replace(".db", "_"+date.today().strftime("%Y%m%d") + ".db"))
            

        
    def init_stocks_ist(self):
        self.root.get_screen('main').ids.main_list.clear_widgets()
        for stock in self.my_stocks.get_stocks():
            self.neue_speise_zeile = TwoLineAvatarIconListItem(
                text=stock[0], 
                secondary_text=str(stock[1]),
                #on_touch_up=self.test
                )
            self.neue_speise_zeile.bind(on_press=self.list_item_click)

            self.root.get_screen('main').ids.main_list.add_widget(self.neue_speise_zeile)

    def list_item_click(self,listitem):
        self.my_stocks.set_current_stock_isin(listitem.secondary_text)
        print(listitem.secondary_text)

        self.root.get_screen('div_details').ids.div_detail_list.clear_widgets()
        for i, row in self.borsaitaliana.get_all_dividends().iterrows():
            #print(f"{row['DataPagamento']}")
            if f"{row['ISIN']}" == self.my_stocks.get_current_stock_isin():
                self.neue_div_zeile = TwoLineAvatarIconListItem(
                    text=f"{row['Provento']}", 
                    secondary_text=f"{row['DataPagamento']}",
                    #on_touch_up=self.test
                    )
                #self.neue_div_zeile.bind(on_press=self.list_item_click)

                self.root.get_screen('div_details').ids.div_detail_list.add_widget(self.neue_div_zeile)


        self.root.current = "div_details"
        self.root.transition.direction = "left"

    def nav_main(self):
        self.root.current = "main"
        self.root.transition.direction = "right"

    def get_date(self, date, start):
        '''
        :type date: <class 'datetime.date'>
        '''
        self.root.get_screen('main').ids.dtEnd.text=str(date)
        print(date)

    def show_date_picker_start(self):
        date_dialog = MDDatePicker(callback=self.get_date)
        date_dialog.open()

    def call_ticker(self):
        dh = self.yahoo
        now = datetime.datetime.strptime(self.root.get_screen('main').ids.dtEnd.text,"%Y-%m-%d")  #datetime.datetime(2021, 1, 28)    # get data up to 6/28/2020
        then = datetime.datetime.strptime(self.root.get_screen('main').ids.dtStart.text,"%Y-%m-%d") #datetime.datetime(2020, 1, 1)        # get data from 01/01/2020
        df = dh.get_ticker_data(self.my_stocks.get_current_stock_ticker(), then, now)
        print(df)

    def call_div(self):
        dh = self.borsaitaliana
        print(dh.count())
        df = dh.get_new_ticker_div(self.my_stocks.get_current_stock_isin())
        print(dh.count())
        print(df.count()[0])
        #print(dh.load())

    def call_all_ticker(self):
        dh = self.yahoo
        now = datetime.datetime.strptime(self.root.get_screen('main').ids.dtEnd.text,"%Y-%m-%d")  #datetime.datetime(2021, 1, 28)    # get data up to 6/28/2020
        then = datetime.datetime.strptime(self.root.get_screen('main').ids.dtStart.text,"%Y-%m-%d") #datetime.datetime(2020, 1, 1)        # get data from 01/01/2020
        df = dh.get_ticker_data(self.root.get_screen('main').ids.txt_ticker.text, then, now)
        print(df)

    def call_all_div(self):
        dh = self.borsaitaliana
        for stock in self.my_stocks.get_stocks():
            df = dh.get_new_ticker_div(self.my_stocks.get_isin_from_name(stock[0]))
            try:
                print(df.count()[0])
            except:
                pass
        #print(dh.load())

    def print_all_div(self):
        dh = self.borsaitaliana
        df = dh.get_all_dividends()
        print(df)
        #print(dh.load())

    def set_csv_path(self):
        self.my_config.set_csv_path(self.root.get_screen('main').ids.txt_ticker.text)
        print(self.my_config.get_csv_path())

    def save_div_csv_div(self):
        dh = self.borsaitaliana
        dh.save_to_csv(self.my_config.get_csv_path() + "div_" + date.today().strftime("%Y%m%d") + ".csv")
        pass

    def test_1(self):
        print(self.my_stocks.get_current_stock_isin())
        print(self.my_stocks.get_current_stock_name())
        #self.root.get_screen('main').ids.btn_test_1.text= "Save All Div ("+str(self.borsaitaliana.get_all_dividends().count()[0])+")"

    def test_2(self):
        pass
    def test_3(self):
        pass
    def test_4(self):
        pass

class GraphDraw(BoxLayout):
    pass
    #def graph(self):
        #xls = pd.read_excel('filepath')
        #df = pd.DataFrame.xls
        #dfgui.show(xls)
        #print xls

if __name__ == '__main__':
    kv = App() 
    kv.run() 