

# 更新
import pymysql


class ConMysql(object):

    def __init__(self,host,user,passwd,database):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.database = database

    def connect_mysql(self):
        self.conn = pymysql.connect(host=self.host,user=self.user,passwd=self.passwd,db=self.database,charset='utf8')
        self.cursor = self.conn.cursor()
        return self.cursor

    #创建数据表
    def create_table(self,tablename,sqlstate):
        self.connect_mysql()
        try:
            self.cursor.execute("DROP TABLE IF EXISTS %s" % tablename)
            self.cursor.execute(sqlstate)
            return "创建数据表成功"
        except:
            return "创建数据表失败"

    #增加数据
    def add_data(self,sqlstate):
        self.__operation(sqlstate)

    #删除数据
    def delete_data(self,sqlstate):
        self.__operation(sqlstate)

    #更新数据
    def update_data(self,sqlstate):
        self.__operation(sqlstate)

    #增加数据，删除数据，更新数据模型
    def __operation(self,sqlstate):
        self.connect_mysql()
        try:
            self.cursor.execute(sqlstate)
            # 提交到数据库执行
            self.conn.commit()
        except:
            # 如果发生错误则回滚
            self.conn.rollback()

    #查询单条数据
    def select_one(self,sqlstate):
        self.connect_mysql()
        try:
            self.cursor.execute(sqlstate)
            results = self.cursor.fetchone()
            return results
        except:
            return "错误,无法提取数据"

    #查询多条数据
    def select_all(self,sqlstate):
        self.connect_mysql()
        try:
            self.cursor.execute(sqlstate)
            # 获取所有记录列表
            results = self.cursor.fetchall()
            datalist = []
            for num in range(0, len(results)):
                datalist.append(results[num])
            return datalist
        except:
            return "错误,无法提取数据"

    #关闭连接
    def close_con(self):
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    host = '127.0.0.1'
    user = 'root'
    passwd = '123456'
    database = 'news'
    conmysql = ConMysql(host,user,passwd,database)
    # sql = """INSERT INTO TEST2(FIRST_NAME,LAST_NAME,AGE,SEX,INCOME) VALUES ('哈哈哈','呵呵呵',98,'男','上海')"""
    sql = """select * from TEST2 where age=100"""
    print(conmysql.select_one(sql))
    conmysql.close_con()