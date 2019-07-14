
import pymysql


class ConMysql(object):

    def __init__(self,host,user,passwd,database):
        self.conn = pymysql.connect(host=host,user=user,passwd=passwd,db=database,charset='utf8')
        self.cursor = self.conn.cursor()

    #创建数据表
    def create_table(self,tablename,sqlstate):
        try:
            self.cursor.execute("DROP TABLE IF EXISTS %s" % tablename)
            self.cursor.execute(sqlstate)
            return "执行成功"
        except:
            return "执行失败"

    #增加数据，删除数据，更新数据
    def operation_data(self,sqlstate):
        try:
            self.cursor.execute(sqlstate)
            # 提交到数据库执行
            self.conn.commit()
            return "执行成功"
        except:
            # 如果发生错误则回滚
            self.conn.rollback()
            return "执行失败，数据回滚"

    #查询数据
    def select_data(self,sqlstate,volume=0):
        # 执行SQL语句
        self.cursor.execute(sqlstate)
        if volume == 0:
            datas = []
            try:
                # 获取所有记录列表
                results = self.cursor.fetchall()
                for row in results:
                    datalist = []
                    for num in range(0,len(row)):
                        datalist.append(row[num])
                    #打印结果
                    datas.append(datalist)
                return datas
            except:
                return "错误,无法提取数据"
        elif volume == 1:
            results = self.cursor.fetchone()
            return results
        else:
            return "错误,volume的值只能为1或者0"
    #关闭连接
    def close_conn(self):
        self.conn.close()
        return "连接关闭"


if __name__ == '__main__':
    host = '127.0.0.1'
    user = 'root'
    passwd = 'root'
    database = 'testmysql'
    conmysql = ConMysql(host,user,passwd,database)
    # sql = """SELECT * FROM EMPLOYEE  WHERE INCOME > %s""" % (1000)
    # print(conmysql.select_data(sql,1))
    sql = """CREATE TABLE TEST2 (
             FIRST_NAME  CHAR(20) NOT NULL,
             LAST_NAME  CHAR(20),
             AGE INT,  
             SEX CHAR(1),
             INCOME FLOAT )"""
    print(conmysql.create_table('TEST2', sql))
    conmysql.close_conn()