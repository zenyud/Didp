# -*- coding: UTF-8 -*-  

# Date Time     : 2019/1/17
# Write By      : adtec(ZENGYU)
# Function Desc : 归档批量初始化
# History       : 2019/1/17  ZENGYU     Create
# Remarks       :
import argparse
import os
import sys
import time

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from archive.db_operator import CommonParamsDao

from archive.archive_enum import AddColumn, DatePartitionRange, OrgPos, \
    PartitionKey
from archive.archive_util import HiveUtil, BizException, DateUtil, StringUtil
from archive.model import DidpMonRunLog
from archive.service import MetaDataService, MonRunLogService

from utils.didp_logger import Logger

LOG = Logger()


class BatchArchiveInit(object):
    """
        批量初始化
    """

    def __init__(self):
        self.__args = self.archive_init()
        self.session = self.get_session()
        self.__print_argument()
        self.pro_start_date = DateUtil.get_now_date_standy()
        self.schema_id = self.__args.schemaID
        self.hive_util = HiveUtil(self.schema_id)
        self.meta_data_service = MetaDataService(self.session)
        self.mon_run_log_service = MonRunLogService(self.session)
        self.schema_id = self.__args.schemaID
        self.source_db = self.__args.sourceDbName
        self.source_table_name = self.__args.sourceTableName
        self.filter_cols = self.__args.filterCol
        self.db_name = self.__args.dbName
        self.table_name = self.__args.tableName
        self.bucket_num = self.__args.bucketsNum
        self.common_dict = self.init_common_dict()

        self.source_ddl = self.meta_data_service. \
            parse_input_table(self.hive_util,
                              self.source_db,
                              self.source_table_name,
                              self.filter_cols)
        self.col_date = self.common_dict.get(AddColumn.COL_DATE.value)
        self.col_org = self.common_dict.get(AddColumn.COL_ORG.value)
        self.org_pos = int(self.__args.orgPos)
        self.data_range = self.__args.dateRange
        self.cluster_col = self.__args.clusterCol
        self.partition_org = self.common_dict.get(PartitionKey.ORG.value)
        self.partition_date_scope = self.common_dict.get(
            PartitionKey.DATE_SCOPE.value)
        self.date_col = self.__args.dateCol
        self.date_format = self.__args.dateColFormat
        self.ignore_err_line = self.__args.ignoreErrLines
        self.now_date = DateUtil.get_now_date_standy()
        self.obj = self.__args.obj
        self.org = self.__args.org
        self.pro_status = 1
        self.error_msg = None

    def get_session(self):
        """
         获取 sqlalchemy 的SESSION 会话
        :return:
        """

        user = os.environ["DIDP_CFG_DB_USER"]
        password = os.environ["DIDP_CFG_DB_PWD"]
        db_url = os.environ["DIDP_CFG_DB_JDBC_URL"]
        x = db_url.index("//")
        y = db_url.index("?")
        db_url = db_url[x + 2:y]

        # db_name = db_login_info['db_name']

        engine_str = "mysql+mysqlconnector://{db_user}:{password}@{db_url}".format(
            db_user=user, password=password,
            db_url=db_url,
        )
        engine = create_engine(engine_str)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

    def init_common_dict(self):
        common_dict = CommonParamsDao(self.session).get_all_common_code()
        if len(common_dict) == 0:
            raise BizException("初始化公共代码失败！请检查数据库")
        else:
            return common_dict

    def __print_argument(self):
        LOG.debug("批量初始化归档")
        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("数据对象名       : {0}".format(self.__args.obj))
        LOG.debug("SCHEMA ID       : {0}".format(self.__args.schemaID))
        LOG.debug("流程ID       : {0}".format(self.__args.proID))
        LOG.debug("系统标识       : {0}".format(self.__args.system))
        LOG.debug("批次号       : {0}".format(self.__args.batch))
        LOG.debug("机构号           : {0}".format(self.__args.org))
        LOG.debug("日期字段           : {0}".format(self.__args.dateCol))
        LOG.debug("日期格式           : {0}".format(self.__args.dateColFormat))
        LOG.debug("源库名           : {0}".format(self.__args.sourceDbName))
        LOG.debug("源表名           : {0}".format(self.__args.sourceTableName))
        LOG.debug("过滤条件         : {0}".format(self.__args.filterSql))
        LOG.debug("过滤字段         : {0}".format(self.__args.filterCol))
        LOG.debug("归档库名          : {0}".format(self.__args.dbName))
        LOG.debug("归档表名          : {0}".format(self.__args.tableName))
        LOG.debug("日期分区范围        : {0}".format(self.__args.dateRange))
        LOG.debug("机构字段位置        : {0}".format(self.__args.orgPos))
        LOG.debug("分桶键             : {0}".format(self.__args.clusterCol))
        LOG.debug("分桶数             : {0}".format(self.__args.bucketsNum))
        LOG.debug("是否忽略错误行           : {0}".format(self.__args.ignoreErrLines))
        LOG.debug("是否补记数据资产           : {0}".format(self.__args.asset))

        LOG.debug("----------------------------------------------")

    @staticmethod
    def archive_init():
        """
            参数初始化
        :return:
        """
        # 参数解析
        parser = argparse.ArgumentParser(description="归档批量初始化")

        parser.add_argument("-obj", required=True, help="数据对象名")
        parser.add_argument("-org", required=True, help="机构")

        parser.add_argument("-sourceDbName", required=True, help="源库名")
        parser.add_argument("-sourceTableName", required=True, help="源表名")
        parser.add_argument("-filterSql", required=False,
                            help="采集过滤SQL条件（WHERE 后面部分）")
        parser.add_argument("-filterCol", required=False, help="过滤字段")
        parser.add_argument("-dateCol", required=True, help="日期字段")
        parser.add_argument("-dateColFormat", required=True, help="日期字段格式")
        parser.add_argument("-schemaID", required=True, help="取连接信息")
        parser.add_argument("-proID", required=True, help="流程ID")
        parser.add_argument("-system", required=True, help="系统标识")
        parser.add_argument("-batch", required=True, help="批次号")
        parser.add_argument("-dbName", required=True, help="归档库名")
        parser.add_argument("-tableName", required=True, help="归档表名")

        parser.add_argument("-dateRange", required=True,
                            help="日期分区范围（N-不分区、M-月、Q-季、Y-年）")
        parser.add_argument("-orgPos", required=True,
                            help="机构字段位置（1-没有机构字段 "
                                 "2-字段在列中 3-字段在分区中）")
        parser.add_argument("-clusterCol", required=True, help="分桶键")
        parser.add_argument("-bucketsNum", required=True, help="分桶数")
        parser.add_argument("-ignoreErrLines", required=False,
                            help="是否忽略错误行（0-否 1-是）")
        parser.add_argument("-asset", required=False, help="是否补记数据资产（0-否 1-是）")
        args = parser.parse_args()
        return args

    def run(self):
        try:
            LOG.info("接入表结构解析，元数据登记 ")
            self.process_ddl()

            if not self.hive_util.exist_table(self.db_name, self.table_name):
                LOG.info("创建归档表 ")
                self.create_table()

            LOG.info("统计日期，并对日期做合法性判断")
            self.count_date()
            LOG.info("查看是否已经做过批量初始化")
            self.check_log()
            LOG.info("开始归档 ")
            self.load()
        except Exception as e:
            self.error_msg = str(e.message)
            self.pro_status = 0
        LOG.info("登记执行日志")
        data_date = DateUtil.get_now_date_format("%Y%m%d")
        pro_end_date = DateUtil.get_now_date_standy()
        source_count = self.hive_util. \
            execute_sql("select count(1) from {source_db}.{source_table}".
                        format(source_db=self.source_db,
                               source_table=self.source_table_name))[0][0]
        archive_count = self.hive_util. \
            execute_sql("select count(1) from {db_name}.{table_name} ".
                        format(db_name=self.db_name,
                               table_name=self.table_name))[0][0]
        reject_lines = int(source_count) - int(archive_count)
        run_log = DidpMonRunLog(PROCESS_ID=self.__args.proID,
                                SYSTEM_KEY=self.__args.system,
                                BRANCH_NO=self.org,
                                BIZ_DATE=data_date,
                                BATCH_NO=self.__args.batch,
                                TABLE_NAME=self.table_name,
                                DATA_OBJECT_NAME=self.obj,
                                PROCESS_TYPE="5",  # 加工类型
                                PROCESS_STARTTIME=self.pro_start_date,
                                PROCESS_ENDTIME=pro_end_date,
                                PROCESS_STATUS=self.pro_status,
                                INPUT_LINES=int(source_count),
                                OUTPUT_LINES=int(archive_count),
                                REJECT_LINES=reject_lines,
                                EXTENDED1="init",  # 记录归档类型
                                ERR_MESSAGE=self.error_msg
                                )
        self.mon_run_log_service.create_run_log(run_log)

        if self.session:
            self.session.close()
        self.hive_util.close()

    def process_ddl(self):
        """
            处理ddl
        :return:
        """
        if len(self.source_ddl) == 0:
            raise BizException("接入数据不存在！ ")
        now_date = DateUtil().get_now_date_standy()
        source_table_comment = self.hive_util. \
            get_table_comment(self.source_db, self.source_table_name)
        # 登记元数据
        self.meta_data_service.upload_meta_data(self.schema_id,
                                                self.db_name,
                                                self.source_ddl,
                                                self.table_name,
                                                now_date,
                                                self.bucket_num,
                                                self.common_dict,
                                                source_table_comment
                                                )

    def create_table(self):
        """
            创建归档表
        :return:
        """
        hql = "CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (" \
              " {col_date} VARCHAR(10) ,". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   col_date=self.col_date
                   )
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " {col_org} VARCHAR(10),".format(
                col_org=self.col_org
            )

        # 构建Body 语句
        body = self.create_table_body(False)
        hql = hql + body + " )"

        part_sql = ""
        # 构建partitioned by
        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            part_sql = "{date_scope} string ,".format(
                date_scope=self.partition_date_scope)
        else:
            raise BizException("日期不分区的表，不需要初始化加载")
        if self.org_pos == OrgPos.PARTITION.value:
            part_sql = part_sql + "{col_org} string,".format(
                col_org=self.partition_org)
        # 若存在分区字段
        if len(part_sql) > 0:
            hql = hql + " PARTITIONED BY ( " + part_sql[:-1] + ")"
        # 分桶
        hql = hql + "clustered by ({CLUSTER_COL}) into {BUCKET_NUM}" \
                    " BUCKETS  STORED AS orc " \
                    "tblproperties('orc.compress'='SNAPPY' ," \
                    "'transactional'='true')". \
            format(CLUSTER_COL=self.cluster_col,
                   BUCKET_NUM=self.bucket_num)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

    def create_table_body(self, is_temp_table):
        """
           构建表的body
       :param is_temp_table: 是否是临时表
       :return: sql str
       """

        sql = ""
        if is_temp_table:
            for field in self.source_ddl:
                sql = sql + "{col_name} string,".format(
                    col_name=field.col_name)
        else:
            for field in self.source_ddl:

                sql = sql + "{col_name} {field_type} ".format(
                    col_name=field.col_name_quote,
                    field_type=field.get_full_type())
                if not StringUtil.is_blank(field.comment):
                    # 看是否有字段备注
                    sql = sql + "comment '{comment_content}'".format(
                        comment_content=field.comment)
                sql = sql + ","
        return sql[:-1]

    def count_date(self):
        """
            检查日期字段的合法性
        :return:
        """
        date_dict = {}
        is_contain_error = False
        date = ""
        hql = " select  from_unixtime(unix_timestamp(`{date_col}`,'{date_format}')," \
              "'yyyyMMdd') as {col_date},count(1) from {source_db}.{source_table_name} " \
              " group by from_unixtime(unix_timestamp(`{date_col}`,'{date_format}'),'yyyyMMdd')  " \
              " order by {col_date} ".format(date_col=self.date_col,
                                             date_format=self.date_format,
                                             col_date=self.col_date,
                                             source_db=self.source_db,
                                             source_table_name=self.source_table_name)
        LOG.debug("执行SQL {0}".format(hql))
        result = self.hive_util.execute_sql(hql)
        for x in result:
            date_str = x[0]
            count = x[1]
            LOG.debug("数据日期：{0}, 数据条数： {1}".format(date, count))
            try:
                date = time.strptime(date_str, "%Y%m%d")
            except Exception as  e:
                LOG.debug("非法日期 ：{0}".format(date))
                is_contain_error = True
                continue
            now_date = DateUtil.get_now_date_format("%Y%m%d")

            if date_str > now_date:
                LOG.debug("日期大于等于今天不合法: {0}".format(date_str))
                is_contain_error = True
                continue
            date_dict[date_str] = count

        if not self.ignore_err_line and is_contain_error:
            raise BizException("数据不合法。如需忽略错误行请调用时采用参数 -IGNORE_ERROR_LINES TRUE")

    def check_log(self):
        """
            查看是否需要继续再做批量初始化
        :return:
        """
        date = DateUtil.get_now_date_format("%Y%m%d")
        run_log = self.mon_run_log_service.find_run_logs(self.table_name,
                                                         self.obj,
                                                         self.org,
                                                         date,
                                                         date
                                                         )
        if run_log:
            raise BizException("当日{0} 已有归档，不能做批量初始化".format(date))

        pass

    def load(self):
        """
            加载数据
        :return:
        """
        hql = " from {source_db}.{source_table} insert into  table " \
              " {db_name}.{table_name} {partition} " \
              " select from_unixtime(unix_timestamp(`{date_col}`,'{date_col_format}')," \
              "'yyyyMMdd') as {col_date}, " \
            .format(source_db=self.source_db,
                    source_table=self.source_table_name,
                    db_name=self.db_name,
                    table_name=self.table_name,
                    partition=self.create_partiton_sql(),
                    date_col=self.date_col,
                    date_col_format=self.date_format,
                    col_date=self.col_date
                    )
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " '{org}',".format(org=self.org)
            pass
        hql = hql + self.build_load_column_sql(None, False) + ","

        def switch_data_range(data_range):
            time_str = "from_unixtime(unix_timestamp(`{date_col}`," \
                       "'{date_format}'),'yyyyMMdd')".format(
                date_col=self.date_col,
                date_format=self.date_format)
            return {
                DatePartitionRange.MONTH.value:
                    " substr({time_str},1,6) as {partition_date_scope}".
                        format(time_str=time_str,
                               partition_date_scope=self.partition_date_scope),
                DatePartitionRange.YEAR.value:
                    " substr({time_str},1,4) as {partition_date_scope}".
                        format(time_str=time_str,
                               partition_date_scope=self.partition_date_scope),
                DatePartitionRange.QUARTER_YEAR.value:
                    " (case  when substr({time_str},5,2)>=01 and "
                    " substr({time_str},5,2) <=03 then "
                    " concat(substr({time_str},1,4),'Q1') "
                    "  when substr({time_str},5,2)>=04 and "
                    " substr({time_str},5,2) <=06 then "
                    " concat(substr({time_str},1,4),'Q2') "
                    "  when substr({time_str},5,2)>=07 and "
                    " substr({time_str},5,2) <=09 then "
                    " concat(substr({time_str},1,4),'Q3') "
                    " when substr({time_str},5,2)>=10 and "
                    " substr({time_str},5,2) <=12 then "
                    " concat(substr({time_str},1,4),'Q4') "
                    " end ) as {partition_date_scope}  ,".
                        format(time_str=time_str,
                               partition_date_scope=self.partition_date_scope
                               )

            }.get(data_range)

        hql = hql + switch_data_range(self.data_range)
        if self.org_pos == OrgPos.PARTITION.value:
            hql = hql + " '{org}' as {partition_org}". \
                format(org=self.org,
                       partition_org=self.partition_org)
        LOG.info("执行SQL:{0}".format(hql[:-1]))
        self.hive_util.execute_with_dynamic(hql[:-1])

    def build_load_column_sql(self, table_alias, need_trim):
        """
            构建column字段sql
        :param table_alias: 表别名
        :param need_trim:
        :return:
        """
        sql = ""
        for field in self.source_ddl:
            if self.source_ddl.index(field) == 0:
                sql = sql + self.build_column(table_alias, field.col_name,
                                              field.data_type,
                                              need_trim)
            else:
                sql = sql + "," + self.build_column(table_alias,
                                                    field.col_name,
                                                    field.data_type,
                                                    need_trim)

        return sql

    @staticmethod
    def build_column(table_alias, col_name, col_type, need_trim):
        """
        :param table_alias:
        :param col_name:
        :param col_type:
        :param need_trim:
        :return:
        """
        result = ""
        if not col_name[0].__eq__("`"):
            col_name = "`" + col_name + "`"
        if StringUtil.is_blank(table_alias):
            result = col_name
        else:
            result = table_alias + "." + col_name
        if need_trim:
            #  如果类型string 做trim操作
            if col_type.upper() in ["STRING", "VARCHAR", "CHAR"]:
                result = "trim({value})".format(value=result)

        return result

    def create_partiton_sql(self):
        hql = ""
        if not StringUtil.eq_ignore(DatePartitionRange.ALL_IN_ONE,
                                    self.data_range):
            hql = hql + " {date_scope},".format(
                date_scope=self.partition_date_scope)
        if self.org_pos == OrgPos.PARTITION.value:
            hql = hql + "{partiton_org} ,".format(
                partiton_org=self.partition_org)

        return " partition (" + hql[:-1] + ")"


if __name__ == '__main__':
    batch_init = BatchArchiveInit()
    batch_init.run()
