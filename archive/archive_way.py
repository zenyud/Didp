# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 归档方式
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
import abc
import argparse
import os
import sys
import traceback
from abc import ABCMeta

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))
from archive.archive_enum import *
from archive.hive_field_info import *
from archive.service import *
from utils.didp_logger import Logger

LOG = Logger()


class ArchiveData(object):
    __metaclass__ = ABCMeta

    """数据归档类
        args:参数
    """

    lock_archive = False  # 归档锁
    lock_meta = False  # 元数据锁
    hds_struct_control = HdsStructControl()
    meta_data_service = MetaDataService()
    mon_run_log_service = MonRunLogService()
    common_dict = {}  # 公共代码字典
    date_scope = ""
    start_date = ""
    end_date = ""
    field_change_list = None  # 变更字段列表
    field_type_change_list = None  # 变更字段类型列表
    source_count = 0  # 原始表数据量
    archive_count = 0  # 归档数据条数
    data_asset_infos = None  # 当日的数据资产列表
    process_type = "5"
    pro_start_date = None  # 流程开始时间
    pro_end_date = None  # 流程结束时间
    pro_status = "1"  # 加工状态
    error_msg = ""  # 错误信息
    is_already_load = False  # 是否已经归档完成

    def __init__(self):
        self.__args = self.archive_init()
        self.hive_util = HiveUtil(self.__args.schemaID)
        self.last_date = DateUtil.get_day_of_day(self.__args.dataDate, -1)
        self.common_dict = self.init_common_dict()
        self.__print_arguments()
        self.source_ddl = self.meta_data_service. \
            parse_input_table(self.args.schemaID,
                              self.args.sourceDbName,
                              self.args.sourceTableName)

        self.db_name = self.args.dbName
        self.table_name = self.args.tableName
        # 日期地段名
        self.col_date = self.common_dict.get(AddColumn.COL_DATE.value)
        # 机构字段名
        self.col_org = self.common_dict.get(AddColumn.COL_ORG.value)
        # 日期分区键
        self.partition_data_scope = self.common_dict.get(
            PartitionKey.DATE_SCOPE.value)
        # 机构分区键
        self.partition_org = self.common_dict.get(PartitionKey.ORG.values)
        self.org_pos = int(self.args.orgPos)
        self.data_range = self.args.dateRange
        self.org = self.args.org
        self.data_date = self.args.dataDate
        self.buckets_num = self.args.bucketsNum
        self.cluster_col = self.args.clusterCol
        self.source_data_mode = int(self.args.sourceDataMode)
        self.system = self.args.system
        self.filter_sql = self.args.filterSql
        self.filter_cols = self.args.filterCol

    @property
    def release_date(self):
        """
            格式化数据日期
        :return:
        """
        __data_date = self.data_date
        __data_date = __data_date[0:4] + "-" + __data_date[4:6] + "-" + \
                      __data_date[6:] + " 00:00:00"
        return __data_date

    # 拉链临时表名
    @property
    def app_table_name1(self):
        app_name = self.common_dict.get("archive.proc.temporary.table")
        if StringUtil.is_blank(app_name):
            app_name = self.table_name + "_hds_tmp"
        if int(self.args.orgPos) != OrgPos.NONE.value:
            app_name = app_name + "_" + self.args.org

        app_name = app_name.replace("OBJ", self.args.obj)
        app_name = app_name.replace("TABLE", self.table_name)
        app_name = app_name + '_1'
        return app_name

    # 是否删除临时表

    @property
    def is_drop_tmp_table(self):
        is_drop_tmp = self.common_dict.get("drop.archive.temp.table")
        if is_drop_tmp:
            return is_drop_tmp
        else:
            return False

    @property
    def input_table_name(self):
        input_table_name = self.common_dict.get(
            "archive.input.temporary.table") if self.common_dict.get(
            "archive.input.temporary.table") else self.table_name + "_hds_input"
        if int(self.args.orgPos) != OrgPos.NONE.value:
            input_table_name = input_table_name + "_" + self.args.org
        return input_table_name

    @property
    def temp_db(self):
        temp_db = self.common_dict.get("archive.temporary.db")
        if not temp_db:
            return self.db_name
        return temp_db

    # 接入表DDL信息


    @property
    def args(self):
        return self.__args

    def init_common_dict(self):
        common_dict = self.hive_util.get_common_dict()
        if len(common_dict) == 0:
            raise BizException("初始化公共代码失败！请检查数据库")
        else:
            return common_dict

    def __print_arguments(self):
        """ 参数格式化输出

        Args:

        Returns:

        Raise:

        """

        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("数据对象名       : {0}".format(self.__args.obj))
        LOG.debug("SCHEMA ID       : {0}".format(self.__args.schemaID))
        LOG.debug("流程ID       : {0}".format(self.__args.proID))
        LOG.debug("系统标识       : {0}".format(self.__args.system))
        LOG.debug("批次号       : {0}".format(self.__args.batch))
        LOG.debug("机构号           : {0}".format(self.__args.org))
        LOG.debug("源数据增全量      : {0}".format(self.__args.sourceDataMode))
        LOG.debug("源库名           : {0}".format(self.__args.sourceDbName))
        LOG.debug("源表名           : {0}".format(self.__args.sourceTableName))
        LOG.debug("主键列表           : {0}".format(self.__args.pkList))
        LOG.debug("过滤条件         : {0}".format(self.__args.filterSql))
        LOG.debug("过滤字段         : {0}".format(self.__args.filterCol))
        LOG.debug("归档库名          : {0}".format(self.__args.dbName))
        LOG.debug("归档表名          : {0}".format(self.__args.tableName))
        LOG.debug("归档方式           : {0}".format(self.__args.saveMode))
        LOG.debug("数据日期           : {0}".format(self.__args.dataDate))
        LOG.debug("日期分区范围        : {0}".format(self.__args.dateRange))
        LOG.debug("机构字段位置        : {0}".format(self.__args.orgPos))
        LOG.debug("分桶键             : {0}".format(self.__args.clusterCol))
        LOG.debug("分桶数             : {0}".format(self.__args.bucketsNum))
        LOG.debug("全量历史表名        : {0}".format(self.__args.allTableName))
        LOG.debug("增量历史表名        : {0}".format(self.__args.addTableName))
        LOG.debug(
            "全量历史表分区范围        : {0}".format(self.__args.allDataRange))
        LOG.debug(
            "增量历史表分区范围        : {0}".format(self.__args.addDataRange))
        LOG.debug("----------------------------------------------")

    @staticmethod
    def archive_init():
        """
            参数初始化
        :return:
        """
        # 参数解析
        parser = argparse.ArgumentParser(description="数据归档组件")

        parser.add_argument("-obj", required=True, help="数据对象名")
        parser.add_argument("-org", required=True, help="机构")
        parser.add_argument("-sourceDataMode", required=True,
                            help="源数据增全量（1-全量 2-增量）")
        parser.add_argument("-sourceDbName", required=True, help="源库名")
        parser.add_argument("-sourceTableName", required=True, help="源表名")
        parser.add_argument("-pkList", required=False, help="主键列表(`|`线分割)")
        parser.add_argument("-filterSql", required=False,
                            help="采集过滤SQL条件（WHERE 后面部分）")
        parser.add_argument("-filterCol", required=False, help="过滤字段")
        parser.add_argument("-schemaID", required=True, help="取连接信息")
        parser.add_argument("-proID", required=True, help="流程ID")
        parser.add_argument("-system", required=True, help="系统标识")
        parser.add_argument("-batch", required=True, help="批次号")
        parser.add_argument("-dbName", required=True, help="归档库名")
        parser.add_argument("-tableName", required=True, help="归档表名")
        parser.add_argument("-saveMode", required=True,
                            help="归档方式（1-历史全量、2-历史增量、"
                                 "4-拉链、5-最近一天增量、6-最近一天全量）")
        parser.add_argument("-dataDate", required=True,
                            help="数据日期（yyyymmdd）")
        parser.add_argument("-dateRange", required=True,
                            help="日期分区范围（N-不分区、M-月、Q-季、Y-年）")
        parser.add_argument("-orgPos", required=True,
                            help="机构字段位置（1-没有机构字段 "
                                 "2-字段在列中 3-字段在分区中）")
        parser.add_argument("-clusterCol", required=True, help="分桶键")
        parser.add_argument("-bucketsNum", required=True, help="分桶数")
        parser.add_argument("-allTableName", required=False,
                            help="全量历史表名（当归档方式为历史增量，"
                                 "且源数据为全量时传入），格式 dbname.tablename")
        parser.add_argument("-addTableName", required=False,
                            help="增量历史表名（当归档方式为历史全量，"
                                 "且源数据为增量时传入），格式 dbname.tablename")
        parser.add_argument("-allDataRange", required=False,
                            help="全量历史表日期分区范围（当归档方式为历史增量，"
                                 "且源数据为全量时传入）,格式同dataRange")
        parser.add_argument("-addDataRange", required=False,
                            help="增量历史表日期分区范围（当归档方式为历史全量，"
                                 "且源数据为增量时传入）,格式同dataRange")
        args = parser.parse_args()
        return args

    def lock(self):
        """
            归档任务锁
        :return:
        """
        if self.hds_struct_control.find_archive(self.__args.obj,
                                                self.__args.org) is None:
            try:
                self.hds_struct_control.archive_lock(self.__args.obj,
                                                     self.__args.org)
                self.lock_archive = True
            except Exception as e:
                raise BizException(
                    "待归档表有另外正在归档的任务或后台数据库更新错，"
                    "请稍后再试。[{0}]".format(e.message))
        else:
            raise BizException("待归档表有另外正在归档的任务，请稍后再试 ")

    def data_partition_check(self):
        """
            日期分区字段检查
        :return:
        """
        if self.hive_util.exist_table(self.__args.dbName,
                                      self.__args.tableName):
            if self.__args.dateRange == DatePartitionRange.ALL_IN_ONE.value \
                    and self.hive_util.has_partition(self.__args.dbName,
                                                     self.__args.tableName):
                raise BizException("归档日期分区与Hive表不一致 ！！！")

        def set_value(x, y, z):
            """ 赋值 """
            return x, y, z

        switch = {
            DatePartitionRange.MONTH.value: set_value(
                self.__args.dataDate[0:6],
                DateUtil().get_month_start(
                    self.__args.dataDate),
                DateUtil().get_month_end(
                    self.__args.dataDate)),

            DatePartitionRange.QUARTER_YEAR.value: set_value(
                DateUtil().get_quarter(self.__args.dataDate),
                DateUtil().get_month_start(
                    self.__args.dataDate),
                DateUtil().get_month_end(
                    self.__args.dataDate)),

            DatePartitionRange.YEAR.value: set_value(self.__args.dataDate[0:4],
                                                     DateUtil().get_year_start(
                                                         self.__args.dataDate),
                                                     DateUtil().get_year_end(
                                                         self.__args.dataDate)),
            DatePartitionRange.ALL_IN_ONE.value: ("", "", "")
        }
        # 获取分区范围，开始日期，结束日期
        x, y, z = switch[self.__args.dateRange]
        self.date_scope = x
        self.start_date = y
        self.end_date = z

    @staticmethod
    def get_data_scope(data_range, data_date):
        if StringUtil.eq_ignore(data_range, DatePartitionRange.MONTH.value):
            return data_date[:6]
        elif StringUtil.eq_ignore(data_range, DatePartitionRange.YEAR.value):
            return data_date[:4]
        elif StringUtil.eq_ignore(data_range,
                                  DatePartitionRange.QUARTER_YEAR.value):
            return DateUtil.get_quarter(data_date)

    def org_check(self):
        if self.hive_util.exist_table(self.__args.dbName,
                                      self.__args.tableName):
            if int(self.__args.orgPos) != self.hive_util.get_org_pos(
                    self.__args.dbName,
                    self.__args.tableName):
                raise BizException("归档机构分区与hive表中不一致 !!!")

    def meta_lock(self):
        """
            元数据处理加锁
        :return:
        """
        start_time = time.time()
        self.lock_meta = False
        self.meta_lock_do()
        while not self.lock_meta:
            if time.time() - start_time > 60000:
                raise BizException("元数据更新等待超时,请稍后再试！")
            try:
                time.sleep(1)
            except Exception as e:
                LOG.debug(e)
            self.meta_lock_do()

    def meta_lock_do(self):

        if not self.hds_struct_control.meta_lock_find(self.__args.obj,
                                                      self.__args.org):
            try:
                self.hds_struct_control.meta_lock(self.__args.obj,
                                                  self.__args.org)

                self.lock_meta = True
            except Exception as e:
                LOG.debug("元数据更新队列等待中 。。。 ")

    def meta_unlock(self):
        """
                   元数据锁解除
               :return:
               """
        if self.lock_meta:
            self.hds_struct_control.meta_unlock(self.__args.obj,
                                                self.__args.org)
            self.lock_meta = False

    def upload_meta_data(self):
        """
            登记元数据
        :return:
        """
        table_comment = self.hive_util.get_table_comment(self.args.sourceDbName,
                                                         self.args.sourceTableName)
        # 先格式化dataDate

        self.meta_data_service.upload_meta_data(self.__args.schemaID,
                                                self.__args.sourceDbName,
                                                self.__args.sourceTableName,
                                                self.__args.dbName,
                                                self.__args.tableName,
                                                self.release_date,
                                                self.__args.bucketsNum,
                                                self.common_dict,
                                                table_comment,
                                                self.filter_cols
                                                )

    @abc.abstractmethod
    def create_table(self):
        """
            创建表
        :return:
        """

    def change_table_columns(self):
        """
                   根据表结构变化增加新的字段
               :return:
               """
        change_detail_buffer = ""

        self.get_fields_rank_list(self.__args.dbName, self.__args.tableName,
                                  self.__args.dataDate)

        if self.field_change_list:
            # 有字段的变化
            for field in self.field_change_list:
                # field type : class FieldState
                if field.hive_no == -2:
                    # hive 需要新增字段
                    change_detail_buffer = change_detail_buffer + \
                                           " `{col_name}` {field_type},". \
                                               format(col_name=field.col_name,
                                                      field_type=field.ddl_type.
                                                      get_whole_type)
                    LOG.debug("field length %s " % field.ddl_type.field_length)
                    LOG.debug("whole_type %s " % field.ddl_type.get_whole_type)
        if len(change_detail_buffer) > 0:
            change_detail_buffer = change_detail_buffer[:-1]  # 去掉末尾的逗号
            alter_sql = "ALTER TABLE {db_name}.{table_name} ADD COLUMNS ({buffer}) ". \
                format(db_name=self.__args.dbName,
                       table_name=self.__args.tableName,
                       buffer=change_detail_buffer)

            LOG.debug("sql 语句为 %s" % alter_sql)
            self.hive_util.execute(alter_sql)
        alter_sql2 = ""
        if self.field_type_change_list:
            # 有字段类型改变
            for field in self.field_type_change_list:
                alter_sql2 = alter_sql2 + "alter table {db_name}.{table_name} " \
                                          "change column `{column}` `{column}` {type} ". \
                    format(db_name=self.__args.dbName,
                           table_name=self.__args.tableName,
                           column=field.col_name,
                           type=field.ddl_type.get_whole_type)
                LOG.debug("field length %s " % field.ddl_type.field_length)
                if not StringUtil.is_blank(field.comment_ddl):
                    # 若备注不为空 则添加备注
                    alter_sql2 = alter_sql2 + " comment '{comment}' ". \
                        format(comment=field.comment_ddl)

                LOG.debug("修改表sql为：%s" % alter_sql2)
                self.hive_util.execute(alter_sql2)

    def get_fields_rank_list(self, db_name, table_name, data_date):
        """
            获取归档数据字段排列结果，将历史元数据信息与现有的HIVE元数据对照比较返回比较结果
        :param db_name:
        :param table_name:
        :param data_date:
        :return:
        """
        meta_field_infos = self.meta_data_service.get_meta_field_info_list(
            table_name, data_date)  # 元数据表中的字段信息

        # Hive 中的字段信息
        hive_field_infos = self.hive_util.get_hive_meta_field(db_name,
                                                              table_name,
                                                              True)

        # 字段名更改列表
        self.field_change_list = self.get_change_list(meta_field_infos,
                                                      hive_field_infos)

        # 检查 字段类型是否改变
        self.field_type_change_list = self. \
            check_column_modify(self.field_change_list)

    @staticmethod
    def get_change_list(meta_field_infos, hive_field_infos):
        """
            获取所有字段
        :param meta_field_infos: 接入表元数据
        :param hive_field_infos: 现有的hive表
        :return:
        """
        hive_field_name_list = [field.col_name.upper() for field in
                                hive_field_infos]
        meta_field_name_list = [x.col_name.upper() for x in
                                meta_field_infos]
        LOG.debug("Hive字段个数为：%s" % len(hive_field_name_list))
        LOG.debug("接入数据字段个数为：%s" % len(meta_field_name_list))
        # 进行对比0
        field_change_list = list()
        for hive_field in hive_field_infos:
            hive_no = hive_field.col_seq  # 字段序号

            if hive_field.col_name.upper() in meta_field_name_list:
                meta_index = meta_field_name_list.index(
                    hive_field.col_name.upper())
                meta_current_no = meta_field_infos[meta_index].col_seq

                ddl_type = MetaTypeInfo(
                    meta_field_infos[meta_index].data_type,
                    meta_field_infos[meta_index].col_length,
                    meta_field_infos[meta_index].col_scale)
                meta_comment = meta_field_infos[meta_index].comment
            else:
                # 源数据中没有Hive的信息
                meta_current_no = -1
                hive_no = -1  # Hive 中有,元数据没有的字段
                ddl_type = None
                meta_comment = None

            hive_type = MetaTypeInfo(hive_field.data_type,
                                     hive_field.col_length,
                                     hive_field.col_scale)

            field_state = FieldState(hive_field.col_name.upper(),
                                     hive_field.col_seq,
                                     meta_current_no,
                                     ddl_type,
                                     hive_type,
                                     hive_field.comment,
                                     meta_comment,
                                     hive_no)

            field_change_list.append(field_state)

        for meta_field in meta_field_infos:
            if meta_field.col_name.upper() not in hive_field_name_list:
                # Hive 里不包含 元数据中的字段
                LOG.debug("meta_field.col_length : %s " % meta_field.col_length)
                ddl_data_type = MetaTypeInfo(meta_field.data_type,
                                             meta_field.col_length,
                                             meta_field.col_scale)

                field_state = FieldState(meta_field.col_name.upper(),
                                         -1,
                                         meta_field.col_seq,
                                         ddl_data_type,
                                         None,
                                         None,
                                         meta_field.comment,
                                         -2  # hive中需要新增
                                         )

                field_change_list.append(field_state)

        change = False  # 判断是否有字段改变 False 无变化 True 有变化
        for field in field_change_list:
            if field.hive_no < 0 or StringUtil.eq_ignore(field.full_seq,
                                                         field.current_seq):
                change = True
        # 如果没有改变 则将field_change_list 置空

        if not change:
            field_change_list = None
        for f in field_change_list:
            LOG.info(
                "field 字段信息：col_name:{col_name}|"
                "ddl_type:{ddl_type}|hive_type:{hive_type}|hive_no:{hive_no}"
                    .format(col_name=f.col_name,
                            ddl_type=f.ddl_type.get_whole_type,
                            hive_type=f.hive_type.get_whole_type,
                            hive_no=f.hive_no))
        return field_change_list

    @staticmethod
    def check_column_modify(field_change_list):
        # type: (list(FieldState)) -> list(FieldState)
        """
            检查字段类型是否发生改变
        :param field_change_list: 字段更改列表
        :return: 更改字段的集合
        """
        is_error = False
        change_fields = set()  # 存放变更的字段
        if field_change_list is not None:
            for field in field_change_list:

                meta_type_hive = field.hive_type
                meta_type_ddl = field.ddl_type
                if not meta_type_hive or not meta_type_ddl:
                    # 新增字段 跳过
                    continue
                if not meta_type_ddl.__eq__(meta_type_hive):
                    # 字段类型不同,判断有哪些不同
                    LOG.debug("meta_type_ddl %s " % meta_type_ddl)
                    LOG.debug("meta_type_hive %s " % meta_type_hive)
                    if StringUtil.eq_ignore(meta_type_ddl.field_type,
                                            meta_type_hive.field_type):
                        # 类型相同判断精度,允许decimal字段精度扩大

                        if meta_type_hive.field_length < meta_type_ddl.field_length \
                                and meta_type_hive.field_scale < meta_type_ddl.field_scale:
                            LOG.debug(
                                "字段{col_name}精度扩大 {hive_type} -->> {ddl_type} \n "
                                "修改表字段精度为 {ddl_type} ".format(
                                    col_name=field.col_name,
                                    hive_type=meta_type_hive.get_whole_type,
                                    ddl_type=meta_type_ddl.get_whole_type))
                            change_fields.add(field)
                            continue

                        elif meta_type_hive.field_length >= meta_type_ddl.field_length \
                                and meta_type_hive.field_scale < meta_type_ddl.field_scale:

                            old_type = meta_type_ddl.get_whole_type
                            meta_type_ddl.field_length = meta_type_hive.field_length
                            LOG.debug(
                                "字段{col_name}精度扩大 {hive_type} -->> {ddl_type1} \n "
                                "修改表字段精度为 {ddl_type2}".format(
                                    col_name=field.col_name,
                                    hive_type=meta_type_hive.get_whole_type,
                                    ddl_type1=old_type,
                                    ddl_type2=meta_type_ddl.get_whole_type
                                ))
                            continue
                        elif meta_type_hive.field_length < meta_type_ddl.field_length \
                                and meta_type_hive.field_scale > meta_type_ddl.field_scale:
                            old_type = meta_type_ddl.get_whole_type
                            meta_type_ddl.field_scale = meta_type_hive.field_scale
                            LOG.debug("字段{col_name}精度扩大 "
                                      "{hive_type} -->> {ddl_type1} \n "
                                      "修改表字段精度为 {ddl_type2}".
                                      format(col_name=field.col_name,
                                             hive_type=meta_type_hive.get_whole_type,
                                             ddl_type1=old_type,
                                             ddl_type2=meta_type_ddl.get_whole_type
                                             ))
                            continue
                        else:
                            LOG.debug("字段{col_name} 精度缩小 "
                                      "{hive_type}-->> {ddl_type}\n"
                                      "不修改归档表字段精度 ！".
                                      format(col_name=field.col_name,
                                             hive_type=meta_type_hive.get_whole_type,
                                             ddl_type=meta_type_ddl.get_whole_type))
                    else:
                        # 不允许字段类型发生改变
                        is_error = True
                        LOG.error("字段{col_name} "
                                  "类型发送变化{hive_type} -->> {ddl_type} !".
                                  format(col_name=field.col_name,
                                         hive_type=meta_type_hive.get_whole_type,
                                         ddl_type=meta_type_ddl.get_whole_type))
                if is_error:
                    raise BizException("源数据字段类型变化过大，请处理后重新归档")

                # 判断字段备注的改变
                if not StringUtil.is_blank(field.comment_ddl):
                    # ddl 中存在备注
                    if StringUtil.is_blank(field.comment_hive):
                        # hive 没有备注
                        LOG.debug("字段{field} 备注发送变化 -->> {comment}".
                                  format(field=field.col_name,
                                         comment=field.comment_ddl))
                        change_fields.add(field)
                    elif not StringUtil.eq_ignore(field.comment_ddl,
                                                  field.comment_hive):
                        LOG.debug(
                            "字段{field} 备注发生变化: {comment1} -->> {comment2} ".
                                format(field=field.col_name,
                                       comment1=field.comment_hive,
                                       comment2=field.comment_ddl))
                        change_fields.add(field)

            ret_fields = list()
            for field in change_fields:
                ret_fields.append(field)
            return ret_fields

    @abc.abstractmethod
    def load_data(self):
        """
            装载数据
        :return:
        """
        pass

    def create_partition_sql(self, partition_range, date_scope, org):
        """
        构建表partition分区
        :param partition_range: 日期分区范围
        :param date_scope: 日期区间
        :param org: 机构分区
        :return:
        """
        s = ""
        result = ""
        if not StringUtil.eq_ignore(partition_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            s = s + "{data_scope_name} = '{date_scope}' ,". \
                format(data_scope_name=self.partition_data_scope,
                       date_scope=date_scope)
        if int(self.args.orgPos) == OrgPos.PARTITION.value:
            s = s + "{partition_key_name} = '{partition_key}' ,". \
                format(partition_key_name=self.partition_org, partition_key=org
                       )
        if len(s) > 0:
            result = "partition ({str})".format(str=s[:-1])

        return result

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

    def create_where_sql(self, table_alias, data_date, data_partition_range,
                         date_scope, org_pos, org, where_from_arg):
        """
            构建表(日期分区+机构)条件语句
        :param table_alias: 表别名
        :param data_date: 数据日期
        :param data_partition_range: 分区范围
        :param date_scope: 日期分区
        :param org_pos: 机构字段位置
        :param org: 机构
        :param where_from_arg : 传入的过滤条件
        :return:  sql str
        """
        tmp_str = ""
        if StringUtil.is_blank(table_alias):
            table_alias = ""
        else:
            table_alias = table_alias + "."
        if not StringUtil.is_blank(data_date):
            tmp_str = tmp_str + "{col_date} = '{value}'".format(
                col_date=self.col_date,
                value=data_date)

        if not StringUtil.eq_ignore(DatePartitionRange.ALL_IN_ONE.value,
                                    data_partition_range):
            if len(tmp_str) > 0:
                tmp_str = tmp_str + "and "
            tmp_str = tmp_str + "{date_scope} = '{value}'".format(
                date_scope=self.partition_data_scope,
                value=date_scope
            )

        if len(tmp_str) > 0:
            tmp_str = tmp_str + "and "
        if not StringUtil.eq_ignore(OrgPos.NONE.value, org_pos):
            col_name = ""
            if StringUtil.eq_ignore(OrgPos.PARTITION.value, org_pos):
                col_name = self.partition_org
            elif StringUtil.eq_ignore(OrgPos.COLUMN.value, org_pos):
                col_name = self.col_org
            tmp_str = tmp_str + table_alias + "{col_name} = '{col_value}'". \
                format(col_name=col_name,
                       col_value=org)
        if where_from_arg:
            if len(tmp_str) > 0:
                tmp_str = tmp_str + "and "
            tmp_str = tmp_str + where_from_arg

        if len(tmp_str) == 0:
            return "1==1"
        else:
            return tmp_str

    def build_load_column_sql(self, table_alias, need_trim):
        """
            构建column字段sql
        :param table_alias: 表别名
        :param need_trim:
        :return:
        """
        sql = ""
        # self.source_ddl = self.meta_data_service.get_meta_field_info_list(
        #     self.table_name, self.data_date)

        if self.field_change_list:
            # 如果字段有变化
            LOG.debug("有字段的变化~ ")
            for field in self.field_change_list:
                is_exists = False
                for ddl_field in self.source_ddl:

                    if StringUtil.eq_ignore(ddl_field.col_name,
                                            field.col_name):
                        is_exists = True
                        break
                if is_exists:
                    if self.field_change_list.index(field) == 0:
                        sql = sql + self.build_column(table_alias,
                                                      field.col_name,
                                                      field.ddl_type.field_type,
                                                      need_trim)

                    else:
                        sql = sql + "," + self.build_column(table_alias,
                                                            field.col_name,
                                                            field.ddl_type.field_type,
                                                            need_trim)
                else:
                    sql = sql + ",''"
        else:
            # 无字段变化的情况
            LOG.debug("无字段的变化 ~ ")
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
            #  如果类型不为string 做trim操作
            if not col_type.upper() in ["STRING", "VARCHAR", "CHAR"]:
                result = "trim({value})".format(value=result)

        return result

    @abc.abstractmethod
    def count_archive_data(self):
        """
            统计归档条数
        :return:
        """

    def delete_exists_archive(self):
        pass

    @abc.abstractmethod
    def register_run_log(self):
        """
            登记执行日志
        :return:
        """
        didp_mon_run_log = DidpMonRunLog(PROCESS_ID=self.args.proID,
                                         SYSTEM_KEY=self.args.system,
                                         BRANCH_NO=self.args.org,
                                         BIZ_DATE=self.data_date,
                                         BATCH_NO=self.args.batch,
                                         TABLE_NAME=self.table_name,
                                         DATA_OBJECT_NAME=self.args.obj,
                                         PROCESS_TYPE=self.process_type,  # 加工类型
                                         PROCESS_STARTTIME=self.pro_start_date,
                                         PROCESS_ENDTIME=self.pro_end_date,
                                         PROCESS_STATUS=self.pro_status,
                                         INPUT_LINES=self.source_count,
                                         OUTPUT_LINES=self.archive_count,
                                         EXTENDED1=self.args.saveMode,  # 记录归档类型
                                         ERR_MESSAGE=self.error_msg
                                         )
        self.mon_run_log_service.create_run_log(didp_mon_run_log)

    def unlock(self):
        self.hds_struct_control.archive_unlock(self.__args.obj, self.__args.org)

    def check_run_log(self, start_date, end_date):
        """
            根据时间 判断是否有归档任务
        :return:
        """
        result = self.mon_run_log_service.find_run_logs(self.table_name,
                                                        self.args.obj,
                                                        self.args.org,
                                                        start_date, end_date)
        if result:
            return True
        else:
            return False

    @staticmethod
    def build_key_sql_on(table_alias1, table_alias2, pk_list):
        """
            更加别名关联主键 例如a.key=b.key
        :param table_alias1:
        :param table_alias2:
        :param pk_list:
        :return:
        """
        hql = ""
        table_alias1 = table_alias1 + "." if not StringUtil.is_blank(
            table_alias1) else ""
        table_alias2 = table_alias2 + "." if not StringUtil.is_blank(
            table_alias2) else ""
        for pk in pk_list:
            if pk_list.index(pk) > 0:
                hql = hql + " and"
            hql = hql + table_alias1 + pk + "=" + table_alias2 + pk
        return hql

    def drop_table(self, db_name, table_name):
        if self.hive_util.exist_table(db_name, table_name):
            self.hive_util.execute(
                "DROP TABLE {0}.{1} ".format(db_name, table_name))

    def run(self):
        """
        归档程序运行入口
        :return:
         1 - 成功 0 - 失败
        """

        LOG.info("------归档作业开始执行------")
        self.pro_start_date = DateUtil.get_now_date_standy()  # 获取流程执行时间
        LOG.info("判断是否有在进行的任务,并加锁 ")
        # self.lock()

        LOG.info("日期分区字段检查 ")
        self.data_partition_check()

        LOG.info("机构字段字段检查 ")
        self.org_check()

        LOG.info("参数初始化2 ")
        self.init_ext()
        LOG.info("元数据处理、表并发处理")
        # self.meta_lock()

        LOG.info("元数据登记与更新")
        self.upload_meta_data()

        if not self.hive_util.exist_table(self.__args.dbName,
                                          self.__args.tableName):
            self.create_table()

        LOG.debug("根据表定义变化信息更新表结构 ")
        self.change_table_columns()

        LOG.info("元数据并发解锁")
        # self.meta_unlock()
        LOG.info("源数据的数据量统计")
        sql = "SELECT COUNT(1) FROM {db_name}.{table_name} ".format(
            db_name=self.__args.sourceDbName,
            table_name=self.__args.sourceTableName)
        self.source_count = int(self.hive_util.execute_sql(sql)[0][0])

        LOG.debug("接入元数据的数据条数为：{0}".format(self.source_count))
        if self.source_count > 0:
            #     # 原始数据不为空
            LOG.info("数据载入")
            try:
                self.load_data()
            except Exception as e:
                LOG.exception(e.message)
                self.error_msg = str(e.message)
                self.pro_status = "0"
            LOG.info("统计入库条数")
            if StringUtil.is_blank(self.error_msg):
                self.archive_count = self.count_archive_data()
            LOG.info("入库的条数为{0}".format(self.archive_count))
            self.pro_end_date = DateUtil.get_now_date_standy()
        else:
            LOG.debug("归档数据为空！ ")
        LOG.info("删除已经存在的数据资产！")
        self.delete_exists_archive()
        LOG.info("登记执行日志")
        self.register_run_log()
        self.is_already_load = True
        LOG.info("解除并发锁")
        LOG.info("归档完成 ! ")
        # self.unlock()
        LOG.info("删除临时表")
        self.clean()

    @abc.abstractmethod
    def init_ext(self):
        pass

    @abc.abstractmethod
    def clean(self):
        pass


class LastAddArchive(ArchiveData):
    """
        最近增量归档
    """

    def clean(self):
        super(LastAddArchive, self).clean()

        pass

    def register_run_log(self):
        super(LastAddArchive, self).register_run_log()

    def init_ext(self):
        if int(self.args.sourceDataMode) != SourceDataMode.ADD.value:
            raise BizException("当日增量不提供非增量数据模式转换存储!")

        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            raise BizException("当日增量归档模式，不允许时间分区 ")
        pass

    def count_archive_data(self):

        hql = "select count(1) from {db_name}.{table_name} ".format(
            db_name=self.args.sourceDbName,
            table_name=self.args.sourceTableName)
        r = self.hive_util.execute_sql(hql)
        return int(r[0][0])

    def create_table(self):
        """
            创建Hive表
        :return:  None
        """
        self.hive_util.execute(
            "DROP TABLE {DB_NAME}.{TABLE_NAME} ".format(
                DB_NAME=self.db_name,
                TABLE_NAME=self.table_name))
        # 获取增加字段
        col_date = self.col_date
        execute_sql = "CREATE TABLE {DB_NAME}.{TABLE_NAME} " \
                      "( {COL_DATE} varchar(10), ". \
            format(DB_NAME=self.db_name,
                   TABLE_NAME=self.table_name,
                   COL_DATE=col_date
                   )  # 原始执行Sql

        # org_pos  1-没有机构字段 2-字段在列中 3-字段在分区中
        if self.org_pos == OrgPos.COLUMN.value:
            execute_sql = execute_sql + "{ORG_COL} string ,". \
                format(ORG_COL=self.col_org)
            # print common_dict.get(AddColumn.COL_ORG.value)
        # 组装字段

        body = self.create_table_body(False)
        execute_sql = execute_sql + body + ")"

        if int(self.args.orgPos) == OrgPos.PARTITION.value:
            # 机构字段在分区中
            execute_sql = execute_sql + " PARTITIONED BY ({org_col} string)". \
                format(org_col=self.partition_org)

        # 默认全部为事务表
        execute_sql = execute_sql + " CLUSTERED  BY ({CLUSTER_COL}) " \
                                    "INTO {BUCKET_NUM} BUCKETS STORED AS ORC " \
                                    "tblproperties('orc.compress'='SNAPPY' ," \
                                    "'transactional'='true')". \
            format(CLUSTER_COL=self.args.clusterCol,
                   BUCKET_NUM=self.buckets_num)
        LOG.debug("建表语句为：%s " % execute_sql)
        self.hive_util.execute(execute_sql)

    def load_data(self):
        pre_table_name = self.args.sourceDbName + "." + \
                         self.args.sourceTableName
        hql = ""

        # 清空表数据
        hql = "TRUNCATE TABLE {DB_NAME}.{TABLE_NAME}".format(
            DB_NAME=self.db_name,
            TABLE_NAME=self.table_name)
        self.hive_util.execute(hql)
        # 插入数据

        hql = "from {source_db_name}.{source_table_name} " \
              "insert into table {db_name}.{table_name} {partition_sql}" \
              "select '{data_date}',". \
            format(source_db_name=self.args.sourceDbName,
                   source_table_name=self.args.sourceTableName,
                   db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(self.data_range,
                                                           self.date_scope,
                                                           self.args.org),
                   data_date=self.data_date
                   )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + " '%s' ," % self.args.org

        # 构造字段的sql
        hql = hql + self.build_load_column_sql("", True)
        if not StringUtil.is_blank(self.filter_sql) :
            # 如果有过滤条件,加入过滤条件
            hql = hql + "WHERE {filter_col}".format(filter_col=self.filter_sql)
        LOG.debug("构造的hql是{0}".format(hql))
        self.hive_util.execute(hql)
        self.archive_count = self.count_archive_data()


class LastAllArchive(ArchiveData):
    """
        最近一天全量
    """

    def clean(self):
        super(LastAllArchive, self).clean()
        pass

    def register_run_log(self):
        super(LastAllArchive, self).register_run_log()
        pass

    # pre_table = ""
    pk_list = None

    def __init__(self):
        super(LastAllArchive, self).__init__()
        self.buildKeySqlOn = None

    def init_ext(self):
        # 判断是否存在主键信息
        pk_list = self.args.pkList
        if not pk_list or StringUtil.is_blank(pk_list):
            raise BizException("归档表{db_name}.{table_name}的主键不存在 ！".
                               format(db_name=self.db_name,
                                      table_name=self.table_name))
        pk_list = pk_list.split("|")
        self.pk_list = [pk.upper() for pk in pk_list]  # 保证主键全部大写
        # 判断前一天是否做过归档
        if self.check_run_log("00000101", self.data_date):
            if not self.check_run_log(self.data_date, self.data_date):
                if not self.check_run_log(self.last_date, self.last_date):
                    raise BizException("前一天没有做归档,不能做最近全量归档 ")

        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            raise BizException(
                "当日全量归档模式，不允许时间分区;当前时间分区为：{0}".
                    format(self.data_range))

    def count_archive_data(self):
        hql = "select count(1) from {db_name}.{table_name} ".format(
            db_name=self.args.sourceDbName,
            table_name=self.args.sourceTableName)
        r = self.hive_util.execute_sql(hql)
        return int(r[0][0])

    def create_table(self):
        col_date = self.col_date
        execute_sql = "CREATE TABLE IF NOT EXISTS " \
                      "{DB_NAME}.{TABLE_NAME} ( {COL_DATE} varchar(10), ". \
            format(DB_NAME=self.db_name,
                   TABLE_NAME=self.table_name,
                   COL_DATE=col_date
                   )  # 原始执行Sql

        # org_pos  1-没有机构字段 2-字段在列中 3-字段在分区中
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            execute_sql = execute_sql + "{ORG_COL} string ,".format(
                ORG_COL=self.col_org)
            # print common_dict.get(AddColumn.COL_ORG.value)
        # 组装字段
        body = self.create_table_body(False)
        execute_sql = execute_sql + body + ")"

        if int(self.args.orgPos) == OrgPos.PARTITION.value:
            # 机构字段在分区中
            execute_sql = execute_sql + " PARTITIONED BY ({org_col} string)". \
                format(org_col=self.partition_org)

        # 默认全部为事务表
        execute_sql = execute_sql + " CLUSTERED  BY ({CLUSTER_COL}) " \
                                    "INTO {BUCKET_NUM} BUCKETS STORED AS ORC " \
                                    "tblproperties('orc.compress'='SNAPPY' ," \
                                    "'transactional'='true')". \
            format(CLUSTER_COL=self.args.clusterCol,
                   BUCKET_NUM=self.buckets_num)
        LOG.debug("建表语句为：%s " % execute_sql)
        self.hive_util.execute(execute_sql)
        pass

    def load_data(self):
        # self.pre_table = self.temp_db + "." + self.input_table_name
        if StringUtil.eq_ignore(self.args.sourceDataMode,
                                SourceDataMode.ADD.value):
            # 增求全
            self.load_data_add()
        else:
            # 全量直接入库
            self.load_data_all()

    def load_data_add(self):
        LOG.info("删除日期为当天的数据")
        hql = "DELETE FROM {db_name}.{table_name} {partition_sql} " \
              "WHERE {col_date} = '{data_date}' ". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(self.data_range,
                                                           self.date_scope,
                                                           self.args.org),
                   col_date=self.col_date,
                   data_date=self.data_date
                   )
        if OrgPos.COLUMN.value == int(self.args.orgPos):
            hql = hql + " and {col_org} = '{org}' ".format(
                col_org=self.col_org,
                org=self.args.org)
        LOG.info("删除数据的SQL为 ：{0}".format(hql))
        self.hive_util.execute(hql)
        LOG.info("根据input表主键删除源表数据")
        hql = "DELETE FROM {db_name}.{table_name} {partition_sql} as A  " \
              "where {where_sql} and " \
              "exists (select 1 from {source_db}.{source_table} as B " \
              "where {build_key_sql_on} )" \
            .format(db_name=self.db_name,
                    table_name=self.table_name,
                    partition_sql=self.create_partition_sql(
                        self.data_range,
                        self.date_scope,
                        self.args.org),
                    where_sql=self.create_where_sql(
                        "A",
                        None,
                        self.data_range,
                        self.date_scope,
                        self.args.orgPos,
                        self.args.org,
                        None),
                    source_db=self.args.sourceDbName,
                    source_table=self.args.sourceTableName,
                    build_key_sql_on=self.build_key_sql_on("A", "B",
                                                           self.pk_list)
                    )
        LOG.info("删除源表数据的SQL为:{0}".format(hql))
        self.hive_util.execute(hql)
        # 插入数据
        LOG.debug("将数据插入归档表")
        hql = "FROM {source_db}.{source_table} " \
              "insert into {db_name}.{table_name}" \
              "{partition_sql} select '{data_date}', ". \
            format(source_db=self.args.sourceDbName,
                   source_table=self.args.sourceTableName,
                   db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(
                       self.data_range,
                       self.date_scope,
                       self.args.org
                   ),
                   data_date=self.data_date)
        if OrgPos.COLUMN.value == int(self.args.orgPos):
            hql = hql + " '{0}', ".format(self.args.org)
        hql = hql + self.build_load_column_sql(None, True)
        if not StringUtil.is_blank(self.filter_sql) :
            # 如果有过滤条件,加入过滤条件
            hql = hql + "WHERE {filter_col}".format(filter_col=self.filter_sql)
        LOG.info("归档的SQL为:{0}".format(hql))
        self.hive_util.execute(hql)

    def load_data_all(self):
        # 先清空数据表
        self.hive_util.execute("TRUNCATE TABLE {db_name}.{table_name}".format(
            db_name=self.db_name,
            table_name=self.table_name))

        LOG.debug("将数据插入归档表")
        hql = "FROM {source_db}.{source_table} " \
              "insert into {db_name}.{table_name}" \
              "{partition_sql} select '{data_date}', ". \
            format(source_db=self.args.sourceDbName,
                   source_table=self.args.sourceTableName,
                   db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(
                       self.data_range,
                       self.date_scope,
                       self.args.org
                   ),
                   data_date=self.data_date)
        if OrgPos.COLUMN.value == int(self.args.orgPos):
            hql = hql + " '{0}', ".format(self.args.org)
        hql = hql + self.build_load_column_sql(None, True)
        if not StringUtil.is_blank(self.filter_sql) :
            # 如果有过滤条件,加入过滤条件
            hql = hql + "WHERE {filter_col}".format(filter_col=self.filter_sql)
        self.hive_util.execute(hql)


class AddArchive(ArchiveData):
    all_table_name = None  # 全量历史表表名db_name.table_name
    all_table_partition_range = None  # 全量历史表分区范围
    all_org_pos = None  # 全量历史表的机构位置
    # save_mode_list = None
    has_table_all = False  # 是否存在全量历史表
    is_drop_app_table = False

    def __init__(self):
        super(AddArchive, self).__init__()

        self.data_mode = self.args.sourceDataMode

    def clean(self):
        pass

    def register_run_log(self):
        super(AddArchive, self).register_run_log()
        pass

    def init_ext(self):
        # 判断SourceDataMode
        if int(self.data_mode) == SourceDataMode.ALL.value:
            # 需要获取All_table_name
            if self.args.allTableName:
                self.all_table_name = self.args.allTableName
                if self.args.allDataRange:
                    self.all_table_partition_range = self.args.allDataRange
                else:
                    raise BizException("全量供数保存增量，全量存储策略日期分区范围参数不合法")
                db_name, table_name = self.all_table_name.split(".")
                self.has_table_all = self.hive_util.exist_table(db_name,
                                                                table_name)
                if self.has_table_all:
                    self.all_org_pos = self.hive_util.get_org_pos(db_name,
                                                                  table_name)

    def count_archive_data(self):
        HQL = "SELECT COUNT(1) FROM {DB_NAME}.{TABLE_NAME} WHERE {WHERE_SQL}". \
            format(DB_NAME=self.db_name,
                   TABLE_NAME=self.table_name,
                   WHERE_SQL=self.create_where_sql("", self.data_date,
                                                   self.data_range,
                                                   self.date_scope,
                                                   self.org_pos,
                                                   self.org,
                                                   None
                                                   ))
        x = self.hive_util.execute_sql(HQL)
        return int(x[0][0])

    def create_table(self):

        hql = "CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (" \
              " {col_date} VARCHAR(10) ". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   col_date=self.col_date
                   )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + " ,{col_org} VARCHAR(10)".format(
                col_org=self.col_date
            )

        # 构建Body 语句
        body = self.create_table_body(False)
        hql = hql + body + " )"

        part_sql = ""
        # 构建partitioned by
        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            part_sql = "{date_scope} string ,".format(
                date_scope=self.partition_data_scope)
        if int(self.args.orgPos) == OrgPos.PARTITION.value:
            part_sql = part_sql + "{col_org} string,".format(
                col_org=self.partition_org)
        # 若存在分区字段
        if len(part_sql) > 0:
            hql = hql + "PARTITIONED BY ( " + part_sql[:-1] + ")"
        # 分桶
        hql = hql + "clustered by ({CLUSTER_COL}) into {BUCKET_NUM}" \
                    " BUCKETS  STORED AS orc " \
                    "tblproperties('orc.compress'='SNAPPY' ," \
                    "'transactional'='true')".format(
            CLUSTER_COL=self.args.clusterCol,
            BUCKET_NUM=self.buckets_num)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

    def change_table_columns(self):
        """
         根据DDL变化信息增加表字段
        :return:
        """
        super(AddArchive, self).change_table_columns()
        if self.all_table_name is None:
            return
        if self.source_data_mode == SourceDataMode.ADD:
            if self.field_change_list:
                # 有增量求全，需要避免表字段变化导致增全量表结构不一致，
                # 两张表字段结构需要一起变动。

                db_name, all_table_name = self.all_table_name.split(".")
                if not self.has_table_all:
                    self.has_table_all = self.hive_util. \
                        exist_table(db_name, all_table_name)
                    if self.has_table_all:
                        self.all_org_pos = self.hive_util. \
                            get_org_pos(db_name, all_table_name)

                if self.has_table_all:
                    meta_field_infos = self.meta_data_service. \
                        get_meta_field_info_list(all_table_name, self.data_date)
                    hive_field_infos = self.hive_util.get_hive_meta_field(
                        db_name,
                        all_table_name,
                        True)
                    field_change_list2 = self.get_change_list(meta_field_infos,
                                                              hive_field_infos
                                                              )
                    field_type_modify_list2 = self. \
                        check_column_modify(field_change_list2)

                    # 判断是否有字段变化
                    change = False
                    for field in field_change_list2:
                        if not StringUtil. \
                                eq_ignore(field.full_seq, field.current_seq) or \
                                        field.hive_no < 0:
                            # 存在字段变化
                            change = True
                    if not change:
                        field_change_list2 = None

                    # 若发生了字段变化
                    tmp_buffer = ""
                    sql = ""
                    if field_change_list2:
                        for field in field_change_list2:
                            if int(field.hive_no) == -2:
                                #  Hive需要新增字段
                                tmp_buffer = " `{col_name}` {col_type} ,". \
                                    format(col_name=field.col_name,
                                           col_type=field.ddl_type.get_whole_type)

                        if len(tmp_buffer) > 0:
                            sql = "ALTER TABLE {TABLE_ALL} " \
                                  "ADD COLUMNS ({COLS})". \
                                format(TABLE_ALL=self.all_table_name,
                                       COLS=tmp_buffer[:-1])
                        LOG.info("执行SQL:{0}".format(sql))
                        self.hive_util.execute(sql)

                    # 发生了字段类型改变
                    if field_type_modify_list2:
                        for field in field_type_modify_list2:
                            sql = "ALTER TABLE {TABLE_ALL} CHANGE COLUMN " \
                                  " `{COL_NAME}` `{COL_NAME}` {COL_TYPE} ". \
                                format(TABLE_ALL=self.all_table_name,
                                       COL_NAME=field.col_name,
                                       COL_TYPE=field.ddl_type.get_whole_type)
                            if not StringUtil.is_blank(field.comment_ddl):
                                sql = sql + " comment '{comment}' ". \
                                    format(comment=field.comment_ddl)
                            LOG.info("执行SQL {0}".format(sql))
                            self.hive_util.execute(sql)

    def load_data(self):
        LOG.debug("先根据数据日期删除表中数据")
        hql = "DELETE FROM  {DB_NAME}.{TABLE_NAME} {PARTITION_SQL} " \
              "WHERE {WHERE_SQL} ". \
            format(DB_NAME=self.db_name,
                   TABLE_NAME=self.table_name,
                   PARTITION_SQL=self.create_partition_sql(self.data_range,
                                                           self.date_scope,
                                                           self.org),
                   WHERE_SQL=self.create_where_sql("", self.data_date,
                                                   self.data_range,
                                                   self.date_scope,
                                                   self.org_pos,
                                                   self.org,
                                                   None)
                   )
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)
        if int(self.data_mode) == SourceDataMode.ADD.value:
            # 直接入库
            self.load_data1()
        elif int(self.data_mode) == SourceDataMode.ALL.value:
            # 全求增转换入库
            self.load_data2()

    def load_data1(self):
        hql = "FROM {source_db}.{source_table} " \
              "insert into {db_name}.{table_name}" \
              "{partition_sql} select '{data_date}', ". \
            format(source_db=self.args.sourceDbName,
                   source_table=self.args.sourceTableName,
                   db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(
                       self.data_range,
                       self.date_scope,
                       self.org
                   ),
                   data_date=self.data_date)
        if OrgPos.COLUMN.value == self.org_pos:
            hql = hql + " '{0}', ".format(self.org)
        hql = hql + self.build_load_column_sql(None, True)
        if not StringUtil.is_blank(self.filter_sql) :
            # 如果有过滤条件,加入过滤条件
            hql = hql + "WHERE {filter_col}".format(filter_col=self.filter_sql)
        LOG.info("执行SQL : {0}".format(hql))
        self.hive_util.execute(hql)

    def load_data2(self):
        """
            全求增 转换入库
        :return:
        """

        meta_info = self.meta_data_service.get_meta_field_info_list(
            self.table_name, self.release_date)

        pk_list = self.args.pkList
        if pk_list:
            pk_list = pk_list.split("|")
            if len(pk_list) < 0:
                raise BizException(
                    "数据对象{obj} 主键不存在 ！".format(obj=self.args.obj))
            else:
                pk_list = [pk.upper() for pk in pk_list]  # 大写
        key_dict = {}
        # 获取主键字典
        for pk in pk_list:
            for field in meta_info:
                if StringUtil.eq_ignore(pk, field.col_name):
                    key_dict[pk] = field.data_type # 只需获取字段类型 无需精度，长度
                    break

        yes_day = DateUtil.get_day_of_day(self.data_date, -1)  # 前一天 str
        # 取得最近一次全量归档信息
        lastest_all_archive = self.mon_run_log_service. \
            find_latest_all_archive(self.system, self.args.obj,
                                    self.args.org, yes_day)
        has_yes_all_data = False  # 判断最近一次全量归档是否在昨日

        if lastest_all_archive:
            biz_date = lastest_all_archive.BIZ_DATE
            if StringUtil.eq_ignore(yes_day, biz_date):
                has_yes_all_data = True

        # 当日全量数据入临时表
        if not self.hive_util.exist_table(self.temp_db,
                                          self.app_table_name1) or \
                not self.hive_util.compare(
                    self.db_name, self.table_name, self.temp_db,
                    self.app_table_name1, False):
            self.drop_table(self.temp_db, self.app_table_name1)

            hql = "CREATE TABLE {temp_db}.{app_name} " \
                  "like {db_name}.{table_name}". \
                format(temp_db=self.temp_db,
                       db_name=self.db_name,
                       app_name=self.app_table_name1,
                       table_name=self.table_name)

        else:
            hql = "DELETE FROM  {temp_db}.{app_name} {partition_sql} " \
                  "where 1=1 ".format(temp_db=self.temp_db,
                                      app_name=self.app_table_name1,
                                      partition_sql=self.create_partition_sql(
                                          self.data_range,
                                          "0",
                                          "0"
                                      ))
        LOG.info("执行SQL:{0}".format(hql))
        self.hive_util.execute(hql)
        if self.is_drop_tmp_table:
            self.is_drop_app_table = True
        # 入临时表语句构建
        hql = "FROM {SOURCE_DB}.{SOURCE_TABLE} INSERT INTO TABLE " \
              "{TEMP_DB}.{APP_TABLE} {PARTITION} " \
              "SELECT '{DATA_DATE}', ". \
            format(SOURCE_DB=self.args.sourceDbName,
                   SOURCE_TABLE=self.args.sourceTableName,
                   TEMP_DB=self.temp_db,
                   APP_TABLE=self.app_table_name1,
                   PARTITION=self.create_partition_sql(self.data_range, "0",
                                                       "0"),
                   DATA_DATE=self.data_date
                   )
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{org}',".format(org=self.org)
        hql = hql + self.build_load_column_sql(None, True)
        if not StringUtil.is_blank(self.filter_sql) :
            # 过滤sql
            hql = hql + " WHERE {0}".format(self.filter_sql)
        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        sql = "FROM {TEMP_DB}.{APP_TABLE} hds_temp_table " \
              "FULL OUTER JOIN  ( ".format(TEMP_DB=self.temp_db,
                                           APP_TABLE=self.app_table_name1)
        if has_yes_all_data:
            # 关联前一日全量数据求增求减
            sql = sql + "SELECT * FROM {ALL_TABLE_NAME} " \
                        "WHERE {WHERE_SQL} ". \
                format(ALL_TABLE_NAME=self.all_table_name,
                       WHERE_SQL=self.create_where_sql(
                           "", yes_day,
                           self.all_table_partition_range,
                           self.get_data_scope(
                               self.all_table_partition_range,
                               yes_day),
                           self.all_org_pos,
                           self.org, None))

        else:
            # 手动生成全量数据
            sql = sql + " SELECT * FROM (SELECT *,row_number() " \
                        "over(distribute by "
            # 按主键分组
            for key in key_dict:
                if str(key_dict.get(key)).lower() in ["string", "varchar",
                                                      "char"]:
                    sql = sql + " trim({key}),".format(key=key)
                else:
                    sql = sql + key + ","
            sql = sql[:-1]

            #  按日期倒序、自定义分区顺序（全量分区-增量/减量分区-初始化分区）排序
            sql = sql + " SORT BY  {COL_DATE} DESC ) " \
                        " AS hds_section_rn FROM ( ".format \
                (COL_DATE=self.col_date)
            # 定位需扫描的分区及自定义分区顺序

            # 最近一次全量
            if lastest_all_archive and self.has_table_all:
                sql = sql + "SELECT {COL_DATE},{COLS} FROM {all_table_name} " \
                            "WHERE {WHERE_SQL} UNION ALL  ". \
                    format(COL_DATE=self.common_dict.
                           get(AddColumn.COL_DATE.value),
                           COLS=self.build_load_column_sql(None, False),
                           all_table_name=self.all_table_name,
                           WHERE_SQL=self.create_where_sql("",
                                                           lastest_all_archive.BIZ_DATE,
                                                           self.all_table_partition_range,
                                                           self.get_data_scope(
                                                               self.all_table_partition_range,
                                                               yes_day),
                                                           self.all_org_pos,
                                                           self.args.org, None
                                                           )
                           )

            # 往日增量
            sql = sql + " SELECT {COL_DATE},{COLS}  FROM {DB_NAME}.{TABLE_NAME}" \
                        " WHERE {COL_DATE}<  '{DATA_DATE}'   " \
                .format(COL_DATE=self.col_date,
                        COLS=self.build_load_column_sql(None, False),
                        DB_NAME=self.db_name,
                        TABLE_NAME=self.table_name,
                        DATA_DATE=self.data_date
                        )

            if lastest_all_archive:
                sql = sql + " AND {COL_DATE} > '{ALL_DATE}' " \
                    .format(COL_DATE=self.col_date,
                            ALL_DATE=lastest_all_archive.BIZ_DATE)
                date_range = self.data_range
                # 确定 Date_scope值
                date_scope = None
                if StringUtil.eq_ignore(date_range,
                                        DatePartitionRange.MONTH.value):
                    date_scope = str(lastest_all_archive.BIZ_DATE)[0:6]
                elif StringUtil.eq_ignore(date_range,
                                          DatePartitionRange.QUARTER_YEAR.value):
                    date_scope = DateUtil.get_quarter(
                        lastest_all_archive.BIZ_DATE)
                elif StringUtil.eq_ignore(date_range,
                                          DatePartitionRange.YEAR.value):
                    date_scope = str(lastest_all_archive.BIZ_DATE)[0:4]

                if date_scope:
                    sql = sql + " AND  {DATE_SCOPE} <= '{VAL1}' AND " \
                                " {DATE_SCOPE} >= '{VAL2}' " \
                        .format(DATE_SCOPE=self.partition_data_scope,
                                VAL1=self.date_scope,
                                VAL2=date_scope
                                )

                # org_pos
                org_pos = int(self.args.orgPos)
                # 确定 ORG_COL
                org_col = None
                if org_pos == OrgPos.PARTITION.value:
                    org_col = self.partition_org
                elif org_pos == OrgPos.COLUMN.value:
                    org_col = self.col_org
                if org_col:
                    sql = sql + " AND {ORG_COL}= '{ORG}' " \
                        .format(ORG_COL=org_col,
                                ORG=self.org)

            sql = sql + " ) hds_section_t1 )  hds_section_t2 " \
                        " WHERE hds_section_t2.hds_section_rn=1 ) " \
                        " hds_section_yes ON  {build_key} " \
                        " INSERT INTO TABLE {DB_NAME}.{TABLE_NAME} " \
                        " {PARTITION} SELECT '{DATA_DATE}',{COLS} " \
                        " WHERE ". \
                format(build_key=self.build_key_sql_on("hds_temp_table",
                                                       "hds_section_yes",
                                                       self.args.pkList),
                       DB_NAME=self.db_name,
                       TABLE_NAME=self.table_name,
                       PARTITION=self.create_partition_sql(self.data_range,
                                                           self.date_scope,
                                                           self.args.org),
                       DATA_DATE=self.data_date,
                       COLS=self.build_load_column_sql("hds_temp_table", False)
                       )
            where_sql = None
            for field in self.source_ddl:
                if field.col_name.upper() not in pk_list:
                    where_sql = "hds_section_yes.{col_name} != " \
                                "hds_temp_table.{col_name} OR".format(
                        col_name=field.col_name_quote
                    )
            if where_sql:
                sql = sql + " {WHERE_SQL} ".format(WHERE_SQL=where_sql)
            sql = sql + " hds_section_yes.{COL_DATE} IS NULL " \
                .format(COL_DATE=self.col_date)

            LOG.info("执行SQL 语句：{0}".format(sql))
            self.hive_util.execute(sql)


class AllArchive(ArchiveData):
    """
        历史全量数据入库
    """
    table_add = None  # 增量历史表表名
    data_range_add = None  # 增量历史表分区范围
    org_pos_add = None  # 增量表机构字段位置
    is_drop_app_table = None

    def __init__(self):
        super(AllArchive, self).__init__()

    def clean(self):
        super(AllArchive, self).clean()
        if self.is_drop_tmp_table:
            self.drop_table(self.temp_db, self.app_table_name1)

    def register_run_log(self):
        super(AllArchive, self).register_run_log()

    def init_ext(self):
        source_data_mode = int(self.args.sourceDataMode)
        if source_data_mode == SourceDataMode.ADD.value:
            #  需要增求全 取增量表表名
            self.table_add = self.args.addTableName
            self.data_range_add = self.args.addDataRange
            if self.table_add:
                if self.data_range_add is None:
                    raise BizException("增量供数保存全量，增量存储策略日期分区范围参数不合法")
                db_name, table_name = self.table_add.split(".")
                self.org_pos_add = self.hive_util.get_org_pos(db_name,
                                                              table_name)

    def count_archive_data(self):
        HQL = "SELECT COUNT(1) FROM {DB_NAME}.{TABLE_NAME} WHERE {WHERE_SQL}". \
            format(DB_NAME=self.db_name,
                   TABLE_NAME=self.table_name,
                   WHERE_SQL=self.create_where_sql("", self.data_date,
                                                   self.data_range,
                                                   self.date_scope,
                                                   self.org_pos,
                                                   self.org,
                                                   None))
        LOG.info("执行SQL：{0}".format(HQL))
        self.hive_util.execute(HQL)

    def create_table(self):
        HQL = " CREATE TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME} ( " \
              " {COL_DATE} VARCHAR(10) ".format(DB_NAME=self.db_name,
                                                TABLE_NAME=self.table_name,
                                                COL_DATE=self.col_date
                                                )
        if self.org_pos == OrgPos.COLUMN.value:
            HQL = HQL + ",{COL_ORG} VARCHAR(10) ".format(COL_ORG=self.col_org)

        for field in self.source_ddl:
            HQL = HQL + ",{field_name} {field_type} ". \
                format(field_name=field.col_name_quote,
                       field_type=field.get_full_type())
            if not StringUtil.is_blank(field.comment):
                HQL = HQL + " comment '{comment}' ".format(
                    comment=field.comment)
        HQL = HQL + " )"
        TMP_SQL = ""
        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            TMP_SQL = "{col_data_scope} string, ".format(
                col_data_scope=self.partition_data_scope)
        if self.org_pos == OrgPos.PARTITION.value:
            TMP_SQL = "{partion_org} string,".format(
                partion_org=self.partition_org)
        if TMP_SQL.__len__() > 0:
            HQL = HQL + " PARTITIONED BY ( {partition} ) ". \
                format(partition=TMP_SQL[:-1])
        HQL = HQL + " clustered by ({CLUSTER_COL}) into {BUCKET_NUM} " \
                    " BUCKETS  STORED AS orc " \
                    " tblproperties('orc.compress'='SNAPPY'," \
                    "'transactional'='true')". \
            format(CLUSTER_COL=self.cluster_col,
                   BUCKET_NUM=self.buckets_num)
        LOG.info("执行SQL:{0}".format(HQL))
        self.hive_util.execute(HQL)
        pass

    def change_table_columns(self):
        super(AllArchive, self).change_table_columns()
        if self.table_add is None:
            return
        if self.source_data_mode == SourceDataMode.ALL:
            if self.field_change_list:
                # 有增量求全，需要避免表字段变化导致增全量表结构不一致，
                # 两张表字段结构需要一起变动。

                db_name, add_table_name = self.table_add.split(".")
                meta_field_infos = self.meta_data_service. \
                    get_meta_field_info_list(add_table_name, self.data_date)
                hive_field_infos = self.hive_util.get_hive_meta_field(
                    db_name,
                    add_table_name,
                    True)
                field_change_list2 = self.get_change_list(meta_field_infos,
                                                          hive_field_infos
                                                          )
                field_type_modify_list2 = self. \
                    check_column_modify(field_change_list2)

                # 判断是否有字段变化
                change = False
                for field in field_change_list2:
                    if not StringUtil. \
                            eq_ignore(field.full_seq, field.current_seq) or \
                                    field.hive_no < 0:
                        # 存在字段变化
                        change = True
                if not change:
                    field_change_list2 = None

                # 若发生了字段变化
                tmp_buffer = ""
                sql = ""
                if field_change_list2:
                    for field in field_change_list2:
                        if int(field.hive_no) == -2:
                            #  Hive需要新增字段
                            tmp_buffer = " `{col_name}` {col_type} ,". \
                                format(col_name=field.col_name,
                                       col_type=field.ddl_type.get_whole_type)

                    if len(tmp_buffer) > 0:
                        sql = "ALTER TABLE {ADD_TABLE} " \
                              "ADD COLUMNS ({COLS})". \
                            format(ADD_TABLE=self.table_add,
                                   COLS=tmp_buffer[:-1])
                    LOG.info("执行SQL:{0}".format(sql))
                    self.hive_util.execute(sql)

                # 发生了字段类型改变
                if field_type_modify_list2:
                    for field in field_type_modify_list2:
                        sql = "ALTER TABLE {ADD_TABLE} CHANGE COLUMN " \
                              " `{COL_NAME}` `{COL_NAME}` {COL_TYPE} ". \
                            format(ADD_TABLE=self.table_add,
                                   COL_NAME=field.col_name,
                                   COL_TYPE=field.ddl_type.get_whole_type)
                        if not StringUtil.is_blank(field.comment_ddl):
                            sql = sql + " comment '{comment}' ". \
                                format(comment=field.comment_ddl)
                        LOG.info("执行SQL {0}".format(sql))
                        self.hive_util.execute(sql)

    def load_data(self):
        #  先根据数据日期删除表中数据
        hql = "DELETE FROM {DB_NAME}.{TABLE_NAME} {PARTITION} " \
              "{WHERE_SQL} ". \
            format(DB_NAME=self.db_name,
                   TABLE_NAME=self.table_name,
                   PARTITION=self.create_partition_sql(self.data_range,
                                                       self.date_scope,
                                                       self.org
                                                       ),
                   WHERE_SQL=self.create_where_sql("", self.data_date,
                                                   self.data_range,
                                                   self.date_scope,
                                                   self.org_pos, self.org,
                                                   None))
        LOG.info("执行SQL:{0}".format(hql))

        # 根据数据源模式执行

        if StringUtil.eq_ignore(self.source_data_mode,
                                SourceDataMode.ALL.value):
            # 直接入库
            self.load_data1()
        elif StringUtil.eq_ignore(self.source_data_mode,
                                  SourceDataMode.ADD.value):
            # 增求全转换入库
            self.load_data2()

    def load_data1(self):
        """
         直接入库
        :return:
        """
        hql = " FROM {SOURCE_DB}.{SOURCE_TABEL_NAME} INSERT INTO TABLE " \
              " {DB_NAME}.{TABLE_NAME} {PARTITION} " \
              " SELECT '{DATA_DATE}',". \
            format(SOURCE_DB=self.args.sourceDbName,
                   SOURCE_TABEL_NAME=self.args.sourceTableName,
                   DB_NAME=self.db_name,
                   TABLE_NAME=self.table_name,
                   PARTITION=self.create_partition_sql(self.data_range,
                                                       self.date_scope,
                                                       self.org),
                   DATA_DATE=self.data_date
                   )
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " '{org}',".format(org=self.org)
        hql = hql + self.build_load_column_sql(None, True)

        if not StringUtil.is_blank(self.filter_sql) :
            # 如果有过滤条件,加入过滤条件
            hql = hql + "WHERE {filter_col}".format(filter_col=self.filter_sql)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

    def load_data2(self):
        """
            增求全转换入库
        :return:
        """
        meta_info = self.meta_data_service.get_meta_field_info_list(
            self.table_name, self.release_date)
        pk_list = self.args.pkList
        if pk_list:
            pk_list = pk_list.split("|")
            if len(pk_list) < 0:
                raise BizException(
                    "数据对象{obj} 主键不存在 ！".format(obj=self.args.obj))
            else:
                pk_list = [pk.upper() for pk in pk_list]  # 大写
        key_dict = {}  # 主键名/主键类型 字典
        # 获取主键字典
        for pk in pk_list:
            for field in meta_info:
                if StringUtil.eq_ignore(pk, field.col_name):
                    key_dict[pk] = field.data_type
                    break
        yes_day = DateUtil.get_day_of_day(self.data_date, -1)  # 前一天

        # 取最近一次全量归档的信息
        lastest_all_archive = self.mon_run_log_service. \
            find_latest_all_archive(self.system, self.args.obj,
                                    self.org, yes_day)
        # 判断最近一次归档是否在昨日
        has_yes_full_data = False
        if lastest_all_archive:
            biz_date = lastest_all_archive.BIZ_DATE
            if StringUtil.eq_ignore(yes_day, biz_date):
                has_yes_full_data = True

        # 写入临时表
        if not self.hive_util.exist_table(self.temp_db,
                                          self.app_table_name1) or \
                not self.hive_util.compare(
                    self.db_name, self.table_name, self.temp_db,
                    self.app_table_name1, False):
            self.drop_table(self.temp_db, self.app_table_name1)

            hql = "CREATE TABLE {temp_db}.{app_name} " \
                  "like {db_name}.{table_name}". \
                format(temp_db=self.temp_db,
                       db_name=self.db_name,
                       app_name=self.app_table_name1,
                       table_name=self.table_name)

        else:
            hql = "DELETE FROM  {temp_db}.{app_name} {partition_sql} " \
                  "where 1=1 ".format(temp_db=self.temp_db,
                                      app_name=self.app_table_name1,
                                      partition_sql=self.create_partition_sql(
                                          self.data_range,
                                          "0",
                                          "0"
                                      ))

        LOG.info("执行SQL:{0}".format(hql))
        self.hive_util.execute(hql)
        if self.is_drop_tmp_table:
            self.is_drop_app_table = True
        # 入临时表SQL
        hql = "FROM {SOURCE_DB}.{SOURCE_TABLE} INSERT INTO TABLE " \
              "{TEMP_DB}.{APP_TABLE} {PARTITION} " \
              "SELECT '{DATA_DATE}', ". \
            format(SOURCE_DB=self.args.sourceDbName,
                   SOURCE_TABLE=self.args.sourceTableName,
                   TEMP_DB=self.temp_db,
                   APP_TABLE=self.app_table_name1,
                   PARTITION=self.create_partition_sql(self.data_range, "0",
                                                       "0"),
                   DATA_DATE=self.data_date
                   )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "'{org}',".format(org=self.org)
        hql = hql + self.build_load_column_sql(None, True)
        if not StringUtil.is_blank(self.filter_sql) :
            # 如果有过滤条件,加入过滤条件
            hql = hql + "WHERE {filter_col}".format(filter_col=self.filter_sql)

        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        hql = " FROM ( SELECT *, row_number() over (distribute by  "
        # 按主键分组
        for key in key_dict:
            if str(key_dict.get(key)).lower() in ["string", "varchar",
                                                  "char"]:
                hql = hql + " trim({key}),".format(key=key)
            else:
                hql = hql + key + ","

        hql = hql[:-1]
        # 按日期倒序、自定义分区顺序（全量分区-增量/减量分区-初始化分区）排序
        hql = hql + " SORT BY {COL_DATE} DESC ) as hds_section_rn FROM (  ". \
            format(COL_DATE=self.col_date)

        if lastest_all_archive:
            hql = hql + "SELECT {COL_DATE},{COLS} FROM {DB_NAME}.{TABLE_NAME} " \
                        "WHERE {WHERE_SQL} " \
                        "UNION ALL ". \
                format(COL_DATE=self.col_date,
                       COLS=self.build_load_column_sql(None, False),
                       DB_NAME=self.db_name,
                       TABLE_NAME=self.table_name,
                       WHERE_SQL=self.create_where_sql("", lastest_all_archive.
                                                       BIZ_DATE,
                                                       self.data_range,
                                                       self.date_scope,
                                                       self.org_pos,
                                                       self.org, None
                                                       )
                       )
        # 当日增量
        hql = hql + " SELECT {COL_DATE},{COLS} FROM {TEMP_DB}.{APP_TABLE} ". \
            format(COL_DATE=self.col_date,
                   COLS=self.build_load_column_sql(None, False),
                   TEMP_DB=self.temp_db,
                   APP_TABLE=self.app_table_name1)
        # 往日增量
        if self.table_add and not has_yes_full_data:
            hql = hql + " UNION ALL " \
                        "SELECT {COL_DATE},{COLS} FROM {TABLE_ADD} " \
                        " WHERE {COL_DATE} < '{DATA_DATE}' ". \
                format(COL_DATE=self.col_date,
                       COLS=self.build_load_column_sql(None, False),
                       TABLE_ADD=self.table_add,
                       DATA_DATE=self.data_date)
            if lastest_all_archive:
                # 有全量数据
                hql = hql + " AND {COL_DATE} > '{ALL_DATE}' ". \
                    format(COL_DATE=self.col_date,
                           ALL_DATE=lastest_all_archive.BIZ_DATE)
                # 拼接分区范围
                date_scope = None
                col_org = None

                if StringUtil.eq_ignore(self.data_range_add,
                                        DatePartitionRange.MONTH.value):
                    date_scope = str(lastest_all_archive.BIZ_DATE)[0:6]
                elif StringUtil.eq_ignore(self.data_range_add,
                                          DatePartitionRange.QUARTER_YEAR.value):
                    date_scope = DateUtil.get_quarter(
                        lastest_all_archive.BIZ_DATE)
                elif StringUtil.eq_ignore(self.data_range_add,
                                          DatePartitionRange.YEAR.value):
                    date_scope = str(lastest_all_archive.BIZ_DATE)[0:4]

                if date_scope:
                    hql = hql + " and {partition_date_scope} <= {date_scope1} " \
                                "and {partition_date_scope} >={date_scope2} ". \
                        format(partition_date_scope=self.partition_data_scope,
                               date_scope1=self.date_scope,
                               date_scope2=date_scope
                               )
                if self.org_pos == OrgPos.COLUMN.value:
                    col_org = self.col_org
                elif self.org_pos == OrgPos.PARTITION.value:
                    col_org = self.partition_org
                if col_org:
                    hql = hql + " and {0} = '{1}' ".format(col_org,
                                                           self.org)

        hql = hql + ") hds_section_t1 " \
                    ") hds_section_t2 " \
                    " insert into table {db_name}.{table_name} " \
                    " {partition} select '{data_date}',". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   partition=self.create_partition_sql(self.data_range,
                                                       self.date_scope,
                                                       self.org),
                   data_date=self.data_date
                   )

        if self.org_pos == OrgPos.COLUMN:
            hql = hql + " '{org}', ".format(org=self.org)

        hql = hql + self.build_load_column_sql(None, False) \
              + " WHERE hds_section_rn=1"
        LOG.info("执行SQL:{0} ".format(hql))
        self.hive_util.execute(hql)


class ChainTransArchive(ArchiveData):
    """
        拉链表归档按月、季度、年分区、不分区 允许出错重跑
    """

    # 是否封链
    is_close_chain = False
    # 闭链后下个分区值
    next_date_scope = None
    # 归档后一天的日期
    next_date = None
    log_head = ""
    pk_list = None
    columns = None
    is_drop_table1 = False
    chain_open_date = "99991231"

    def __init__(self):
        super(ChainTransArchive, self).__init__()
        self.chain_sdate = self.common_dict.get(AddColumn.CHAIN_SDATE.value)
        self.chain_edate = self.common_dict.get(AddColumn.CHAIN_EDATE.value)
        self.data_mode = self.args.sourceDataMode
        # 拉链表不做比较的字段

    @property
    def not_compare_column(self):
        not_compare_column = self.common_dict.get("chain.notcompare.column")
        return not_compare_column

    def clean(self):
        super(ChainTransArchive, self).clean()
        if self.is_already_load:
            if self.is_drop_tmp_table:
                self.drop_table(self.temp_db, self.app_table_name1)
        pass

    def register_run_log(self):
        super(ChainTransArchive, self).register_run_log()

    def init_ext(self):
        pk_list = self.args.pkList
        if not pk_list or StringUtil.is_blank(pk_list):
            raise BizException("归档表{db_name}.{table_name}的主键不存在 ！".format(
                db_name=self.db_name,
                table_name=self.table_name))
        self.pk_list = [pk.upper() for pk in pk_list.split("|")]

        if self.check_run_log("00000101", self.data_date):
            if not self.check_run_log(self.data_date, self.data_date):
                if not self.check_run_log(self.last_date, self.last_date):
                    raise BizException("前一天没有做归档,不能做最近全量归档 ")
        else:
            self.data_mode = SourceDataMode.ADD.value
        LOG.info(
            "归档表{db_name}.{table_name} 日期：{data_date} "
            "以前未归档，归档模式按照 :{data_mode}  "
                .format(db_name=self.db_name,
                        table_name=self.table_name,
                        data_date=self.data_date,
                        data_mode=self.data_mode))
        self.log_head = "结构化数据归档[表{db_name}.{table_name} " \
                        "机构：{org} 日期：{data_date} " \
                        "数据源模式:{data_mode} 归档方式：{save_mode} ]" \
            .format(db_name=self.db_name,
                    table_name=self.table_name,
                    org=self.args.org,
                    data_date=self.data_date,
                    data_mode=self.data_mode,
                    save_mode=self.args.saveMode
                    )
        #  分区值和封链判断
        if StringUtil.eq_ignore(DatePartitionRange.MONTH.value,
                                self.data_range):
            if self.end_date.__eq__(self.data_date):
                self.is_close_chain = True
                self.next_date = DateUtil.get_day_of_day(self.data_date, 1)
                self.next_date_scope = self.next_date[0:6]
        if StringUtil.eq_ignore(DatePartitionRange.QUARTER_YEAR.value,
                                self.data_range):
            if self.end_date.__eq__(self.data_date):
                self.is_close_chain = True
                self.next_date = DateUtil.get_day_of_day(self.data_date, 1)
                self.next_date_scope = DateUtil.get_quarter(self.next_date)

        if StringUtil.eq_ignore(DatePartitionRange.YEAR.value,
                                self.data_range):
            if self.end_date.__eq__(self.data_date):
                self.is_close_chain = True
                self.next_date = DateUtil.get_day_of_day(self.data_date, 1)
                self.next_date_scope = self.next_date[0:4]

        # 不需要做拉链的字段
        if not StringUtil.is_blank(self.not_compare_column):
            self.columns = self.not_compare_column.split(",")

    def count_archive_data(self):
        hql = "select count(1) from {db_name}.{table_name} ".format(
            db_name=self.args.sourceDbName,
            table_name=self.args.sourceTableName)
        r = self.hive_util.execute_sql(hql)
        return int(r[0][0])

    def create_table(self):
        hql = "CREATE TABLE IF NOT EXISTS {db_name}.{table_name} " \
              " ({chain_sdate} varchar(10),{chain_edate} varchar(10), ". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   chain_sdate=self.chain_sdate,
                   chain_edate=self.chain_edate
                   )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "{col_org} string ,".format(
                col_org=self.col_org)
        hql = hql + self.create_table_body(False) + ")"
        tmp_buf = ""
        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            tmp_buf = self.partition_data_scope + \
                      " string,"
        if int(self.args.orgPos) == OrgPos.PARTITION.value:
            tmp_buf = tmp_buf + "{partion_org} string,". \
                format(partion_org=self.partition_org)
        if tmp_buf.__len__() > 0:
            hql = hql + "partitioned by (" + tmp_buf[:-1] + ")"

        hql = hql + "clustered by ({clusterCol}) into {bucketsNum} " \
                    "BUCKETS  STORED AS orc " \
                    "tblproperties('orc.compress'='SNAPPY' ," \
                    "'transactional'='true')". \
            format(clusterCol=self.args.clusterCol,
                   bucketsNum=self.buckets_num)
        self.hive_util.execute(hql)

    def load_data(self):
        if int(self.data_mode) == SourceDataMode.ALL.value:
            self.load_data_trans_all()
        else:
            self.load_data_trans_add()

        # 是否封链
        if self.is_close_chain:
            self.close_trans_chain()

    def load_data_trans_all(self):
        LOG.debug("----------------拉链表归档--------------")

        # 更新闭链区的数据
        hql = "UPDATE  {db_name}.{table_name} {partition_sql} set " \
              "{chain_edate} = '{chain_open_date}' where " \
            .format(db_name=self.db_name,
                    table_name=self.table_name,
                    partition_sql=self.create_partition_sql(
                        self.data_range,
                        self.date_scope,
                        self.args.org
                    ),
                    chain_edate=self.chain_edate,
                    chain_open_date=self.chain_open_date

                    )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "{col_org} = '{org}' and ".format(
                col_org=self.col_org,
                org=self.org
            )
        hql = hql + self.chain_edate + "= '{0}'".format(self.data_date)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("先删除开区间当天的数据开链日期")
        hql = "DELETE FROM {db_name}.{table_name} {partition_sql} " \
              "where ".format(db_name=self.db_name,
                              table_name=self.table_name,
                              partition_sql=self.create_partition_sql(
                                  self.data_range,
                                  self.date_scope,
                                  self.args.org
                              ))
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "{col_org} = '{org}' and ".format(
                col_org=self.col_org,
                org=self.args.org
            )
        hql = hql + self.chain_edate + "= '{0}'".format(self.data_date)
        self.hive_util.execute(hql)

        # 存入临时表
        if not self.hive_util.exist_table(self.temp_db,
                                          self.app_table_name1) or \
                not self.hive_util.compare(
                    self.db_name, self.table_name, self.temp_db,
                    self.app_table_name1, False):
            # drop 表
            self.drop_table(self.temp_db, self.app_table_name1)
            hql = "CREATE TABLE {temp_db}.{app_name} " \
                  "like {db_name}.{table_name}". \
                format(temp_db=self.temp_db,
                       db_name=self.db_name,
                       app_name=self.app_table_name1,
                       table_name=self.table_name)

        else:
            hql = "DELETE FROM  {temp_db}.{app_name} {partition_sql} " \
                  " where 1=1 ".format(temp_db=self.temp_db,
                                       app_name=self.app_table_name1,
                                       partition_sql=self.create_partition_sql(
                                           self.data_range,
                                           "0",
                                           "0"
                                       ))
        self.hive_util.execute(hql)
        if self.is_drop_tmp_table:
            self.is_drop_table1 = True

        LOG.debug("1、通过主键关联将变化量插入临时分区")
        hql = "INSERT INTO TABLE  {temp_db}.{app_table_name} {partition_sql} " \
              "SELECT A.{chain_sdate},'{data_date}'," \
            .format(temp_db=self.temp_db,
                    app_table_name=self.app_table_name1,
                    partition_sql=self.create_partition_sql(self.data_range,
                                                            self.date_scope,
                                                            self.args.org),
                    chain_sdate=self.chain_sdate,
                    data_date=self.data_date
                    )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "'{org}' ,".format(org=self.args.org)

        hql1 = " (SELECT * FROM {db_name}.{table_name} WHERE " \
               "{where_sql}  and {chain_edate} = '{chain_open_date}' ) A  " \
               "LEFT JOIN  {source_db}.{source_table} B ON ( " \
            .format(db_name=self.db_name,
                    table_name=self.table_name,
                    where_sql=self.create_where_sql(None, None,
                                                    self.data_range,
                                                    self.date_scope,
                                                    self.args.orgPos,
                                                    self.args.org,
                                                    None
                                                    ),
                    chain_edate=self.chain_edate,
                    chain_open_date=self.chain_open_date,
                    source_db=self.args.sourceDbName,
                    source_table=self.args.sourceTableName
                    )
        hql = hql + self.build_load_column_sql("A", False) + "FROM " + hql1

        # 主键关联
        hql = hql + self.build_key_sql_on("A", "B", self.pk_list) + \
              ") WHERE CONCAT_WS('|', {col1}) != CONCAT_WS('|',{col2}) " \
              "UNION ALL SELECT '{data_date}','{CHAIN_OPENDATE}'," \
                  .format(col1=self.build_sql_column_with_not_compare("A"),
                          col2=self.build_sql_column_with_not_compare("B"),
                          data_date=self.data_date,
                          CHAIN_OPENDATE=self.chain_open_date)
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "'{0}',".format(self.args.org)
        hql = hql + self.build_load_column_sql("A", False) + \
              "FROM {source_db}.{source_table_name} A " \
              "LEFT JOIN (SELECT * FROM {db_name}.{table_name} " \
              "WHERE {where_sql} and {chain_edate} ='{chain_open_date}' ) B " \
              "ON  ({on_key}) " \
              " WHERE  CONCAT_WS('|',{col1}) != CONCAT_WS('|',{col2}) " \
                  .format(source_db=self.args.sourceDbName,
                          source_table_name=self.args.sourceTableName,
                          db_name=self.db_name,
                          table_name=self.table_name,
                          where_sql=self.create_where_sql(None,
                                                          None,
                                                          self.data_range,
                                                          self.date_scope,
                                                          self.args.orgPos,
                                                          self.args.org,
                                                          None),
                          chain_edate=self.chain_edate,
                          chain_open_date=self.chain_open_date,
                          on_key=self.build_key_sql_on("A", "B", self.pk_list),
                          col1=self.build_sql_column_with_not_compare("A"),
                          col2=self.build_sql_column_with_not_compare("B")
                          )

        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("2、通过临时表主键（闭链为当天）删除正式表闭链时间99991231中数据")
        hql = "DELETE FROM {db_name}.{table_name} {partition_sql} AS A " \
              " WHERE {where_sql} AND EXISTS " \
              "(SELECT * FROM {temp_db}.{app_table} AS B " \
              " WHERE {key_list} AND {chain_edate} ='{data_date}' ) " \
              "AND {chain_edate} = '{chain_open_date}' " \
            .format(db_name=self.db_name,
                    table_name=self.table_name,
                    partition_sql=self.create_partition_sql(
                        self.data_range,
                        self.date_scope,
                        self.args.org
                    ),
                    where_sql=self.create_where_sql("A", None,
                                                    self.data_range,
                                                    self.date_scope,
                                                    self.args.orgPos,
                                                    self.args.org,
                                                    None
                                                    ),
                    temp_db=self.temp_db,
                    app_table=self.app_table_name1,
                    key_list=self.build_key_sql_on("B", "A", self.pk_list),
                    chain_edate=self.chain_edate,
                    data_date=self.data_date,
                    chain_open_date=self.chain_open_date
                    )

        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("3、临时分区数据插入插入正式分区")
        hql = "INSERT INTO TABLE  {db_name}.{table_name} {partition_sql} " \
              "select {chain_sdate},{chain_edate},". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(self.data_range,
                                                           self.date_scope,
                                                           self.args.org),
                   chain_sdate=self.chain_sdate,
                   chain_edate=self.chain_edate
                   )
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{0}', ".format(self.org)
        hql = hql + self.build_load_column_sql(None, False) + \
              "FROM {temp_db}.{app_table}".format(temp_db=self.temp_db,
                                                  app_table=self.app_table_name1)
        if not StringUtil.is_blank(self.filter_sql) :
            # 如果有过滤条件,加入过滤条件
            hql = hql + "WHERE {filter_col}".format(filter_col=self.filter_sql)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

    def load_data_trans_add(self):
        LOG.debug("----------------拉链表归档--------------")
        LOG.debug("更新当天闭链区的数据为99991231")
        hql = "UPDATE {db_name}.{table_name} {partition_sql} " \
              "SET {chain_edate} = '{chain_open_date}' WHERE ". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(self.data_range,
                                                           self.date_scope,
                                                           self.args.org),
                   chain_edate=self.chain_edate,
                   chain_open_date=self.chain_open_date
                   )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "{col_org} = '{org}' AND ". \
                format(col_org=self.col_org,
                       org=self.org)
        hql = hql + "{chain_edate} = '{data_date}' ". \
            format(chain_edate=self.chain_edate,
                   data_date=self.data_date)
        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("先删除开区间当天的数据开链日期")
        hql = "DELETE FROM {db_name}.{table_name} {partition_sql} " \
              " WHERE ".format(db_name=self.db_name,
                               table_name=self.table_name,
                               partition_sql=self.create_partition_sql(
                                   self.data_range,
                                   self.date_scope,
                                   self.args.org)
                               )
        if int(self.args.org) == OrgPos.COLUMN.value:
            hql = hql + " {col_org} = '{org}' AND ".format(
                col_org=self.col_org,
                org=self.args.org
            )
        hql = hql + "{chain_edate} = '{data_date}'". \
            format(chain_edate=self.chain_edate,
                   data_date=self.data_date)
        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("写入闭区间")
        hql = "UPDATE {db_name}.{table_name} {partition_sql} AS A " \
              "SET {chain_edate} = '{data_date}'  " \
              " WHERE  {where_sql} AND EXISTS " \
              "(SELECT 1 FROM {source_db}.{source_table} AS B " \
              " WHERE {pk_sql}) AND " \
              "{chain_edate} = '{chain_open_date}'". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(self.data_range,
                                                           self.date_scope,
                                                           self.args.org),
                   chain_edate=self.chain_edate,
                   data_date=self.data_date,
                   where_sql=self.create_where_sql("A", None,
                                                   self.data_range,
                                                   self.date_scope,
                                                   self.args.orgPos,
                                                   self.args.org, None),
                   source_db=self.args.sourceDbName,
                   source_table=self.args.sourceTableName,
                   pk_sql=self.build_key_sql_on("A", "B", self.pk_list),
                   chain_open_date=self.chain_open_date
                   )
        LOG.info("执行SQL {0}".format(hql))
        self.hive_util.execute(hql)
        LOG.debug("写入开区间")
        hql = "INSERT INTO TABLE {db_name}.{table_name} {partition_sql} " \
              "SELECT '{data_date}','{chain_open_date}', ". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   partition_sql=self.create_partition_sql(self.data_range,
                                                           self.date_scope,
                                                           self.args.org),
                   data_date=self.data_date,
                   chain_open_date=self.chain_open_date
                   )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "'{0}', ".format(self.args.org)
        hql = hql + self.build_load_column_sql(None, False) + \
              " FROM {source_db}.{source_table}". \
                  format(source_db=self.args.sourceDbName,
                         source_table=self.args.sourceTableName
                         )
        if not StringUtil.is_blank(self.filter_sql) :
            # 如果有过滤条件,加入过滤条件
            hql = hql + "WHERE {filter_col}".format(filter_col=self.filter_sql)
        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)
        LOG.debug("----------------拉链表归档完成--------------")

    def close_trans_chain(self):
        LOG.debug("------------------------封链开始----------------------------")
        LOG.debug("先删除新分区数据")
        hql = "DELETE FROM {db_name}.{table_name} {partition_sql} " \
              "WHERE ".format(db_name=self.db_name,
                              table_name=self.table_name,
                              partition_sql=self.create_partition_sql(
                                  self.data_range,
                                  self.next_date_scope,
                                  self.args.org)
                              )
        if int(self.args.org) == OrgPos.COLUMN.value:
            hql = hql + " {col_org} = '{org}' AND". \
                format(col_org=self.col_org,
                       org=self.args.org)
        hql = hql + "{chain_edate} >= '{data_date}' ". \
            format(chain_edate=self.chain_edate,
                   data_date=self.data_date)
        self.hive_util.execute(hql)

        LOG.debug("并将99991231数据写入到下一分区中")
        hql = "INSERT INTO TABLE {db_name}.{table_name} {partiton} " \
              "SELECT '{data_date}','{chain_open_date}', ". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   partiton=self.create_partition_sql(self.data_range,
                                                      self.next_date_scope,
                                                      self.args.org),
                   data_date=self.data_date,
                   chain_open_date=self.chain_open_date)

        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + "'{0}',".format(self.args.org)
        hql = hql + self.build_load_column_sql(None, False) + \
              "FROM {db_name}.{table_name} WHERE {where_sql} AND " \
              "{chain_edate} = '{chain_open_date}'". \
                  format(db_name=self.db_name,
                         table_name=self.table_name,
                         where_sql=self.create_where_sql(None, None,
                                                         self.data_range,
                                                         self.date_scope,
                                                         self.args.orgPos,
                                                         self.args.org, None),
                         chain_edate=self.chain_edate,
                         chain_open_date=self.chain_open_date
                         )
        self.hive_util.execute(hql)
        LOG.debug("----------------------------封链完成------------------------")
        pass

    def build_sql_column_with_not_compare(self, table_alias):
        """
        构建加载SQL中列字段部分不包含字段字段
        :return:
        """
        sql = ""
        # 有DDL 变化
        if self.field_change_list:
            for field in self.field_change_list:
                is_exists = False
                for source_field in self.source_ddl:
                    if StringUtil.eq_ignore(field.col_name,
                                            source_field.col_name):
                        is_exists = True
                        break
                if is_exists:
                    if self.contain_column(field.col_name):
                        continue
                    if self.field_change_list.index(field) == 0:
                        sql = sql + self.build_column1(table_alias,
                                                       field.col_name,
                                                       field.ddl_type)
                    else:
                        sql = sql + "," + self.build_column1(table_alias,
                                                             field.col_name,
                                                             field.ddl_type)
                else:
                    sql = sql + ",'' "

        else:
            for field in self.source_ddl:
                if self.contain_column(field.col_name):
                    continue
                field_type = MetaTypeInfo(field.data_type,
                                          field.col_length,
                                          field.col_scale)
                if self.source_ddl.index(field) == 0:
                    sql = sql + self.build_column1(table_alias, field.col_name,
                                                   field_type)
                else:
                    sql = sql + "," + self.build_column1(table_alias,
                                                         field.col_name,
                                                         field_type)

        return sql

    @staticmethod
    def build_column1(table_alias, col_name, col_type):
        if col_name[0].__eq__('`'):
            col_name = '`' + col_name + '`'
        ret = table_alias + '.' + col_name if not StringUtil.is_blank(
            table_alias) else col_name
        ret = "nvl(CAST( {ret} as {col_type}  ),'')". \
            format(ret=ret,
                   col_type=col_type.get_whole_type)
        return ret

    def contain_column(self, tmp_col):
        v = False
        if self.columns:
            for col in self.columns:
                if StringUtil.eq_ignore(tmp_col, col):
                    v = True
                    break
        return v


if __name__ == '__main__':
    a = ChainTransArchive()
    a.run()
