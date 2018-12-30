import numpy as np
import pandas as pd
import requests
import urllib
import datetime as dt
from utilities.config import weibo_config
import time

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
        search_res = res.json()

        now_dt = dt.datetime.now()
        search_res['crawl_time'] = now_dt.strftime('%Y-%m-%d %H:%M:%S')
        search_res['keyword'] = kword

        if typ == 'df':
            search_res = self.search_dict2df(search_res)
        return search_res

    def search_many_pages(self, kword, pages=10, typ='df', interval=0.5):
        '''
        爬取某 关键词（kword）搜索结果的前p页， 调用search_one_page()
        :param kword:
        :param pages:
        :param typ:
        :return: 返回json_list或df
        '''
        self._login()
        if typ not in ['df', 'dict']:
            print('search_many_pages的参数typ有误，请确认！')
            return

        print('开始搜索...')
        print('关键词：【{}】'.format(kword))
        print('搜索总页数：【{}】'.format(pages))
        search_res = []
        for p in range(1, pages + 1):
            search_res.append(self.search_one_page(kword, p, 'dict'))
            time.sleep(interval)

        if typ == 'df':
            search_res = [self.search_dict2df(d) for d in search_res]
            search_res = pd.concat(search_res, ignore_index=True)
            search_res.drop_duplicates('mid', inplace=True)
            print('总条数：【{}】'.format(len(search_res)))
        return search_res

    def comment_one_page(self, mid, p, typ='df'):
        '''
        获取某 mid 的第p页上的评论
        :param mid:
        :param p:
        :param typ:
        :return:
        '''
        self._login()
        if typ not in ['dict', 'df']:
            print('comment_one_page的参数typ有误，请确认！')
            return

        print(p)
        url = self.comm_base_url.format(mid, p)
        comm = self.sess.get(url).json()

        now_dt = dt.datetime.now()
        comm['crawl_time'] = now_dt.strftime('%Y-%m-%d %H:%M:%S')

        if typ == 'df':
            self.comm_dict2df(comm)
            comm = pd.DataFrame(comm['data']['data'])
        return comm

    def comment_all_pages(self, mid, typ='df', interval=0.5):
        # 获取某 mid 对应的全部评论
        self._login()
        if typ not in ['dict', 'df']:
            print('mid_comm_crawl_all_pages的参数typ有误，请确认！')
            return

        p = 1
        comm_all = []
        while 1:
            comm = self.comment_one_page(mid, p, 'dict')
            if 'data' in comm:
                comm_all.append(comm)
                time.sleep(interval)
                p += 1
            else:
                break

        if typ == 'df':
            comm_all = [self.comment_dict2df(comm) for comm in comm_all]
            comm_all = pd.concat(comm_all, ignore_index=True)
            print('mid={} 的微博共爬取到 {}条 评论'.format(mid, len(comm_all)))
        return comm_all

    @staticmethod
    def gen_search_q(kword):
        ''' search_base_url中应使用url编码过的关键词 '''
        q = '=1&q=' + kword
        q = urllib.parse.quote(q)
        return q

    @classmethod
    def search_dict2df(self, dict_res):
        ''' 将 dict格式的关键词搜索结果转化为需要的 df '''

        final = []
        need_cols = ['created_at', 'mid', 'text', 'reposts_count', 'comments_count', 'attitudes_count']
        cards = dict_res['data']['cards']

        card_groups = []
        for card in cards:
            card_groups += card['card_group']

        for card_g in card_groups:
            if 'mblog' not in card_g:
                continue
            log = card_g['mblog']
            final.append([log[c] for c in need_cols])
        final = pd.DataFrame(final, columns=need_cols)
        final['keyword'] = dict_res['keyword']
        str_now_dt = dict_res['crawl_time']
        final['crawl_time'] = str_now_dt
        now_dt = dt.datetime.strptime(str_now_dt, '%Y-%m-%d %H:%M:%S')
        final['std_created_at'] = final['created_at'].apply(lambda x: Weibo_Crawler.created_at_to_time(x, now_dt))
        return final

    @staticmethod
    def comment_dict2df(dict_res):
        final = pd.DataFrame(dict_res['data']['data'])
        now_dt = dict_res['created_time']

        str_now_dt = dict_res['crawl_time']
        final['crawl_time'] = str_now_dt
        now_dt = dt.datetime.strptime(str_now_dt, '%Y-%m-%d %H:%M:%S')
        final['std_created_at'] = final['created_at'].apply(lambda x: Weibo_Crawler.created_at_to_time(x, now_dt))
        return final

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
