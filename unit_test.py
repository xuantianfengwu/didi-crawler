from Crawlnalyzer.db_manager.mysql_manager import MysqlHelper

class Unit_Test:
    def __init__(self):
        self.unit_names = ['mysqlhelper']

    def test_mysqlhelper(self):

        print('Start Testing MysqlHelper...')
        mysqlhelper = MysqlHelper()

        print('\ncase1:')
        data = mysqlhelper.query_data('show tables')
        print(data)

        print('\ncase2:')
        count = mysqlhelper.execute_sql('show tables')
        print(count)

        print('\ncase3:')
        count = mysqlhelper.execute_sql('select * from weibo_search_comments')
        print(count)

        print('\ncase4:')
        df1 = mysqlhelper.query_data_as_dataframe('select * from weibo_search_comments limit 10', cols=[])
        print(df1.shape)
        print(df1.columns)

        print('\ncase5:')
        cols = mysqlhelper.query_table_cols('weibo_search_comments')
        print(cols)

        print('\ncase6:')
        df2 = mysqlhelper.query_whole_table_as_dataframe('weibo_search_comments')
        print(df2.shape)
        print(df2.columns)


if __name__ == '__main__':
    ut = Unit_Test()
    ut.test_mysqlhelper()