import pymysql
from sqlalchemy import create_engine
import pandas as pd
from utilities.config import mysql_config

class MysqlHelper:
    '''python操作mysql的的封装'''

    def __init__(self): #, ):
        '''
        初始化参数
        :param host:        主机
        :param user:        用户名
        :param password:    密码
        :param database:    数据库
        :param port:        端口号，默认是3306
        :param charset:     编码，默认是utf8
        '''

        self.host = mysql_config['host']
        self.port = mysql_config['port']
        self.database = mysql_config['database']
        self.user = mysql_config['user']
        self.password = mysql_config['password']
        self.charset = mysql_config['charset']
        self.conn = None
        self.cur = None
        self.__connect(**mysql_config)
        self.all_tables = [x[0] for x in self.query_data('show tables')]
        # pymysql:
        # self.engine = create_engine(str(r"mysql+pymysql://%s:"+'%s'+"@%s/%s?charset=%s") % (user, password, host, database))

        # mysqlclient:
        # self.engine = create_engine( "mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(user, password, host, port, database, charset))

    def __connect(self,host, user, password, database, port=3306, charset='utf8mb4'):
        '''
        获取连接对象和执行对象
        :return:
        '''
        if self.cur == None:
            try:
                if self.conn == None:
                    self.conn = pymysql.connect(host=self.host,
                                                user=self.user,
                                                password=self.password,
                                                database=self.database,
                                                port=self.port,
                                                charset=self.charset)
                self.cur = self.conn.cursor()
            except Exception as err:
                print('连接失败！')
                print(err)

    def close(self):
        '''
        关闭执行工具和连接对象
        '''
        if self.cur != None:
            self.cur.close()
            self.cur = None
        #if self.conn != None:
        #    self.conn.close()

    def query_data(self, sql, params=None):
        '''
                根据sql和参数获取数据
                :param sql:          sql语句
                :param params:       sql语句对象的参数元组，默认值为None
                :return:             查询的一行数据
        '''
        data = None
        try:
            count = self.cur.execute(sql, params)
            if count != 0:
                data = self.cur.fetchall()
            else:
                data = []
        except Exception as ex:
            print(ex)
        # finally:
        #    self.close()
        return data

    def execute_sql(self, sql, params=None):
        '''
        执行增删改专用
        :param sql:           sql语句
        :param params:        sql语句对象的参数列表，默认值为None
        :return:              受影响的行数
        '''
        count = None
        try:
            count = self.cur.execute(sql, params)
            self.conn.commit()
        except Exception as ex:
            print(ex)
        # finally:
        #    self.close()
        return count

    def query_data_as_dataframe(self, sql, params=None, cols=[]):
        '''
        根据sql语句获取相应内容，格式为dataframe
        :param sql:
        :param params:
        :param cols:
        :return: dataframe
        '''
        data = self.query_data(sql, params)
        df = pd.DataFrame(list(data))
        add_cols = cols + list(df.columns)  # cols不够长自动填充
        df.columns = add_cols[:df.shape[1]]
        return df

    def query_table_cols(self, table):
        '''
        获取table的全部列名
        :param table:
        :return: list
        '''
        #data = self.query_data('select COLUMN_NAME from information_schema.COLUMNS where table_name = %s',params=table)
        data = self.query_data('show columns from %s' % table)
        data = [d[0] for d in data]
        return data

    def query_whole_table_as_dataframe(self, table):
        '''
        获取table的全部数据，格式为dataframe
        :param table:
        :return:
        '''
        data = self.query_data('select * from %s ' % table)
        cols = self.query_table_cols(table)
        df = pd.DataFrame(list(data))
        df.columns=cols
        return df

    def upload_df_to_db(self, df, table, if_exists='replace'):
        '''
        将dataframe上传到数据库内，
        若不存在会自动新建，若存在则根据if_exists参数（replace，append）决定是覆盖还是增加，
        :param df:
        :param table:
        :param if_exists:
        :return:
        '''
        if table not in self.all_tables:
            cols = df.columns
            self.execute_sql('create table {}({} text)'.format(table, ' text,'.join(cols)))

        if if_exists == 'replace':
            self.execute_sql('truncate table {}'.format(table))
        elif if_exists == 'append':
            pass

        row_amt, col_amt = df.shape
        sql = 'insert into {} values({})'.format(table, ','.join(['%s'] * col_amt))
        params = []
        for i, row in df.iterrows():
            params.append([str(r) for r in row])
        print('即将插入{}条数据到{}'.format(row_amt, table))
        count = self.cur.executemany(sql, params)
        self.conn.commit()
        print('成功插入{}条～'.format(count))
        return count

    def df_to_table2(self, df, table, if_exists='replace'):
        try:
            pd.io.sql.to_sql(df, table, self.engine, schema=self.database, if_exists=if_exists, index=False)
            # df.astype(str).to_sql(table,con=self.engine,if_exists=if_exists,index=False)
        except Exception as e:
            print(e)


'''   examples  '''

