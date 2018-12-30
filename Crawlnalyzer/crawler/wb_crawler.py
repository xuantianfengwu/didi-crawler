import numpy as np
import pandas as pd
import requests
import urllib
import datetime as dt
from Crawlnalyzer.config.config import weibo_config

class Weibo_Crawler(object):
    def __init__(self):

        self.username = weibo_config['user']
        self.password = weibo_config['passwd']

        self.login_url =  'https://passport.weibo.cn/sso/login'
        self.search_base_url = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type{}&page_type=searchall&page={}'
        self.comm_base_url =   'https://m.weibo.cn/api/comments/show?id={}&page={}'

        self.sess = None

    def __gen_login_headers_and_data(self):
        login_data = {'username': self.username,
                      'password': self.password,
                      'savestate': '1',
                      'r': 'https://m.weibo.cn/',
                      'ec': '0',
                      'pagerefer': 'https://m.weibo.cn/login?backURL=https%253A%252F%252Fm.weibo.cn%252F',
                      'entry': 'mweibo',
                      'wentry': '',
                      'loginfrom': '',
                      'client_id': '',
                      'code': '',
                      'qq': '',
                      'mainpageflag': '1',
                      'hff': '',
                      'hfp': ''}
        login_headers = {
            # 'Cookie': '_T_WM=b9c463e2ded23be9a76ef4ea8e73bd84; login=d8d48c30708fe9bafe16817c6ffc3bd1; WEIBOCN_FROM=1110006030; MLOGIN=0; M_WEIBOCN_PARAMS=uicode%3D10000011%26fid%3D102803',
            'Host': 'passport.weibo.cn',
            'Origin': 'https://passport.weibo.cn',
            'Referer': 'https://passport.weibo.cn/signin/login?entry=mweibo&res=wel&wm=3349&r=https%3A%2F%2Fm.weibo.cn%2F',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        return login_headers, login_data

    def _login(self):
        ''' 登录检查 '''
        if self.sess == None:
            try:
                login_headers, login_data = self.__gen_login_headers_and_data()
                self.sess = requests.Session()
                self.sess.post(self.login_url, data=login_data, headers=login_headers)
                print('登录成功！')
            except Exception as ex:
                print('登录失败！')
                print(ex)

    @staticmethod
    def gen_search_q(kword):
        ''' search_base_url中应使用url编码过的关键词 '''
        q = '=1&q=' + kword
        q = urllib.parse.quote(q)
        return q

    def search_one_page(self, kword, p, typ='df'):
        '''
         爬取某 关键词（kword）的搜索结果的第p页
        :param kword:
        :param p:
        :param typ:
        :return: 返回类型可选【df,json】
        '''
        self._login()
        if typ not in ['df', 'dict']:
            print('search_crawl_one_page的参数typ有误，请确认！')
            return

        print('搜索页数：p={}'.format(p))
        q = self.gen_search_q(kword)
        url = self.search_base_url.format(q, p)
        res = self.sess.get(url)
        dict_search_res = res.json()

        now_dt = dt.datetime.now()
        dict_search_res['crawl_time'] = now_dt.strftime('%Y-%m-%d %H:%M')

        if typ == 'df':
            df_search_res = self.search_res_dict_to_df(dict_search_res)
            df_search_res = self.df_add_derivative_cols(df_search_res, now_dt)
            return df_search_res
        else:
            return dict_search_res

    def search_crawl_all_pages(self, kword, pages=10, typ='df'):
        # 爬取某 关键词（kword）搜索结果的前p页， 调用search_crawl_one_page()， 返回json_list或df
        self._login()
        if typ not in ['df', 'dict']:
            print('search_crawl_all_pages的参数typ有误，请确认！')
            return

        print('开始搜索...')
        print('关键词：【{}】'.format(kword))
        print('搜索总页数：【{}】'.format(pages))
        if typ == 'df':
            df_search_reses = [self.search_crawl_one_page(kword, p, typ) for p in range(1, pages + 1)]
            df_search_res = pd.concat(df_search_reses, ignore_index=True)
            return df_search_res
        else:
            dict_search_res = {'page={}'.format(p): self.search_crawl_one_page(kword, p, typ)
                               for p in range(1, pages + 1)}
            return dict_search_res

    def search_res_dict_to_df(self, dict_res):
        # 将 dict格式的关键词搜索结果转化为需要的 df
        final = []
        need_cols1 = ['edit_at', 'mid', 'text', 'reposts_count', 'comments_count', 'attitudes_count']
        need_cols2 = ['created_at', 'mid', 'text', 'reposts_count', 'comments_count', 'attitudes_count']
        cards = dict_res['data']['cards']

        card_groups = []
        for card in cards:
            card_groups += card['card_group']

        for card_g in card_groups:
            if 'mblog' not in card_g:
                continue
            log = card_g['mblog']
            final.append([log[c] for c in need_cols2])
        final = pd.DataFrame(final, columns=need_cols2)
        return final

    def mid_comm_crawl_one_page(self, mid, p, typ='df'):
        # 获取某 mid 的第p页上的评论
        self._login()
        if typ not in ['dict', 'df']:
            print('mid_comm_crawl_one_page的参数typ有误，请确认！')
            return

        url = self.comm_url.format(mid, p)
        json_comm = self.sess.get(url)
        dict_comm = json_comm.json()
        now_dt = dt.datetime.now()
        dict_comm['crawl_time'] = now_dt.strftime('%Y-%m-%d %H:%M:%S')
        if typ == 'df':
            df_comm = pd.DataFrame(dict_comm['data']['data'])
            df_comm = self.df_add_derivative_cols(df_comm, now_dt)
            del df_comm['liked']
            return df_comm
        return dict_comm

    def mid_comm_crawl_all_pages(self, mid, typ='df'):
        # 获取某 mid 对应的全部评论
        self._login()
        if typ not in ['dict', 'df']:
            print('mid_comm_crawl_all_pages的参数typ有误，请确认！')
            return

        p = 1
        dict_comm_final = {}
        while 1:
            dict_comm = self.mid_comm_crawl_one_page(mid, p, 'dict')
            if 'data' in dict_comm:
                print(p)
                dict_comm_final['page={}'.format(p)] = dict_comm
                p += 1
            else:
                break
        print('mid={} 的微博共爬取到 {}页 评论'.format(mid, p - 1))
        if typ == 'df':
            df_comm = [self.mid_comm_dict_to_df(dict_comm_final['page={}'.format(p)]) for p in range(1, p)]
            df_comm = pd.concat(df_comm, ignore_index=True)
            print('mid={} 的微博共爬取到 {}条 评论'.format(mid, len(df_comm)))
            return df_comm
        else:
            return dict_comm_lst

    @staticmethod
    def mid_comm_dict_to_df(dict_comm):
        df_comm = pd.DataFrame(dict_comm['data']['data'])
        del df_comm['liked']
        now_dt = dt.datetime.strptime(dict_comm['crawl_time'], '%Y-%m-%d %H:%M:%S')
        df_comm = Weibo_Crawler.df_add_derivative_cols(df_comm, now_dt)
        return df_comm

    @staticmethod
    def created_at_to_time(created_at, now_dt):
        # 将微博的发布时间「created time」转化为标准时间
        if created_at.endswith('分钟前'):
            minutes = int(created_at[:-3])
            create_dt = now_dt - dt.timedelta(seconds=minutes * 60)
            f_create_dt = create_dt.strftime('%Y-%m-%d %H:%M')
        elif created_at.endswith('小时前'):
            hours = int(created_at[:-3])
            create_dt = now_dt - dt.timedelta(hours=hours)
            f_create_dt = create_dt.strftime('%Y-%m-%d %H:%M')
        elif created_at[:2] == '昨天':
            date = (now_dt.date() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
            hr = created_at[-4:]
            f_create_dt = '{} {}'.format(date, hr)
        else:
            f_create_dt = '{}-{}'.format(now_dt.year, created_at)
        return f_create_dt

    @staticmethod
    def df_add_derivative_cols(df, now_dt):
        df['crawl_time'] = now_dt.strftime('%Y-%m-%d %H:%M:%S')
        df['std_created_at'] = df['created_at'].apply(lambda x: Weibo_Crawler.created_at_to_time(x, now_dt))
        return df