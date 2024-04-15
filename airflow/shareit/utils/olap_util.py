import jpype
from airflow.configuration import conf as ds_conf
class OLAP(object):

    def __init__(self):
        self.jdbc_driver_path = ds_conf.get("olap","driver_path")
        self.jdbc_username = ds_conf.get("olap","username")
        self.jdbc_password = ds_conf.get("olap","password")
        self.jdbc_url = ds_conf.get("olap","url")


    def execute_jdbc_driver(self,sql):
        jpype.startJVM(jpype.getDefaultJVMPath(), '-Djava.class.path=' + self.jdbc_driver_path,convertStrings=False)
        java_sql_DriverManager = jpype.java.sql.DriverManager
        conn = java_sql_DriverManager.getConnection(self.jdbc_url, self.jdbc_username, self.jdbc_password)
        stmt = conn.createStatement()
        stmt.executeQuery(sql)
        conn.close()
        jpype.shutdownJVM()



if __name__ == '__main__':
    sql = '''--conf bdp-query-engine=spark-submit-sql-3\n CREATE TABLE IF NOT EXISTS iceberg.zll_test.datacake_activetable_has_pk (activeName STRING COMMENT '', activeType STRING COMMENT '', creatTime STRING COMMENT '', id INT COMMENT '', activeName1 STRING COMMENT '') USING iceberg LOCATION "s3://ads-cold-ue1/table_location/ue1/zll_test/datacake_activetable_has_pk"'''
    olap = OLAP()
    olap.execute_jdbc_driver(sql)

