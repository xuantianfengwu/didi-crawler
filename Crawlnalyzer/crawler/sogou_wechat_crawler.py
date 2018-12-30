import wechatsogou

# 可配置参数

# 直连
ws_api = wechatsogou.WechatSogouAPI()

# 验证码输入错误的重试次数，默认为1
ws_api = wechatsogou.WechatSogouAPI(captcha_break_time=3)

# 所有requests库的参数都能在这用
# 如 配置代理，代理列表中至少需包含1个 HTTPS 协议的代理, 并确保代理可用
ws_api = wechatsogou.WechatSogouAPI(proxies={
    "http": "127.0.0.1:8888",
    "https": "127.0.0.1:8888",
})

# 如 设置超时
ws_api = wechatsogou.WechatSogouAPI(timeout=0.1)

res_json = ws_api.search_article('滴滴出行')
mg_helper = MongoHelper()
mg_helper.upload_json_data_list(res_json, 'wechatsogou_data', 'search_article')
res_df = pd.DataFrame([r['article'] for r in res_json])
res_df['time'] = res_df['time'].apply(lambda x:dt.datetime.fromtimestamp(x))
res_df