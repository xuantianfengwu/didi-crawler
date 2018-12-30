import pymongo


class MongoHelper(object):
    def __init__(self):
        self.host = "mongodb+srv://xuezhihuan:!QAZ1qaz@cluster0-vfhz3.azure.mongodb.net/test?retryWrites=true"
        self.client = None

    def __connect(self):
        if self.client == None:
            self.client = pymongo.MongoClient(self.host)

    def upload_json_data(self, json_data, db_name, coll_name):
        self.__connect()
        mydb = self.client[db_name]
        mycoll = mydb[coll_name]
        # mycoll.insert_many(json_data)
        res = mycoll.insert_one(json_data)
        print('已插入，id={}'.format(res.inserted_id))
        return res

    def upload_json_data_list(self, json_data_list, db_name, coll_name):
        self.__connect()
        mydb = self.client[db_name]
        mycoll = mydb[coll_name]
        res = mycoll.insert_many(json_data_list)
        print('已插入，共{}条数据～'.format(len(json_data_list)))
        return res

    def list_all_databases(self):
        self.__connect()
        return self.client.list_database_names()

    def list_all_collections(self):
        self.__connect()
        return self.client[db_name].list_collection_names()
