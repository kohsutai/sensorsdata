# -*- coding: utf-8 -*-
import argparse
import datetime
import os
import shutil
import subprocess
import sys
import time
import traceback
from configparser import ConfigParser

import pymysql
import json
import requests


class Log:
    def __init__(self):
        cp = ConfigParser()
        config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../config/configuration.conf")
        cp.read(config_path)
        self.logsDir = cp.get("log", "logs_dir")
        self.saveTime = cp.get("log", "save_time")
        today = datetime.date.today().strftime("%Y%m%d")
        log_dir = os.path.join(self.logsDir, today)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.log_file = open(
            os.path.join(log_dir, "dataTransfer.log"), "a+")

    def deal_error(self, e):
        """ 处理错误异常
            参数：
                e：异常
        """
        log_str = '发生错误: %s' % e
        self.write_log(log_str)

    def write_log(self, log_str):
        """ 记录日志
            参数：
                log_str：日志
        """
        date_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        format_log_str = "%s ---> %s \n " % (date_now, log_str)
        print(format_log_str)
        self.log_file.write(format_log_str)

    def del_logs(self):
        try:
            overdue = (datetime.date.today() + datetime.timedelta(days=-int(self.saveTime))).strftime("%Y%m%d")
            overdue_file = os.path.join(self.logsDir, overdue)
            if os.path.exists(overdue_file):
                shutil.rmtree(overdue_file)
                self.write_log("删除过期日志：%s" % overdue_file)
        except Exception as e:
            pass

    def close(self):
        self.del_logs()
        self.log_file.close()


# 定义全局的变量
log = Log()


# 判断是否为active namenode
def get_active_namenode(name_nodes,port):
    for name_node in name_nodes.split(","):
        resp = requests.get(f"http://{name_node}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
        json_dic = json.loads(resp.text)
        log.write_log(f"namenode state info: " + resp.text)
        if json_dic["beans"][0]["State"] == 'active':
            return name_node
    raise EnvironmentError('No active namenode exists!')
    sys.exit(1)


class MysqlClient:
    def __init__(self, host, port, database, user, password):
        self.connection = pymysql.connect(host, user, password, database, int(port))
        self.cursor_obj = self.connection.cursor()

    @staticmethod
    def get_mysql_config(mysql_url: str):
        url_list = mysql_url.split("/")
        host = url_list[2].split(":")[0]
        port = url_list[2].split(":")[1]
        db = url_list[3]
        return host, int(port), db

    def to_dict_iter(self):
        columns = [datum[0] for datum in self.cursor_obj.description]
        for row in self.cursor_obj:
            yield dict(zip(columns, row))

    def sql_query(self, sql):
        self.cursor_obj.execute(sql)
        result = self.to_dict_iter()
        return result

    def close(self):
        if self.cursor_obj:
            self.cursor_obj.close()
        if self.connection:
            self.connection.close()


class BaseTransfer:
    root_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../")
    all_config = ConfigParser()
    all_config.read(os.path.join(root_path, "config/configuration.conf"))
    # 讀取配置文件
    cluster_config = dict(all_config.items("cluster"))
    mysql_config = dict(all_config.items("mysql"))
    sa_config = dict(all_config.items("sa"))
    program_config = dict(all_config.items("program"))
    # 解析出配置文件的参
    # source_cluster_hdfs_host = cluster_config["source_cluster_hdfs_host"]
    source_cluster_hdfs_hosts = cluster_config["source_cluster_hdfs_hosts"]
    #source_cluster_hdfs_host = get_active_namenode(source_cluster_hdfs_hosts)
    source_namenode_webui_port = cluster_config["source_namenode_webui_port"]
    source_cluster_hdfs_host = get_active_namenode(source_cluster_hdfs_hosts,source_namenode_webui_port)
    

    source_cluster_hdfs_port = cluster_config["source_cluster_hdfs_port"]
    # target_cluster_hdfs_host = cluster_config["target_cluster_hdfs_host"]
    target_cluster_hdfs_host = cluster_config["target_cluster_hdfs_hosts"]
    # target_cluster_hdfs_host = get_active_namenode(target_cluster_hdfs_hosts)

    target_cluster_hdfs_port = cluster_config["target_cluster_hdfs_port"]
    source_hdfs_cache_path = cluster_config["source_hdfs_cache_path"]

    target_hdfs_cache_path = cluster_config["target_hdfs_cache_path"]

    impala_machine_host = cluster_config["impala_machine_host"]
    impala_kerberos_service_name = cluster_config["impala_kerberos_service_name"]
    hive_trust_user = cluster_config["hive_trust_user"]
    hive_machine_host = cluster_config["hive_machine_host"]

    source_hdfs_local_path = cluster_config["source_hdfs_local_path"]
    target_ssh_trust_user = cluster_config["target_ssh_trust_user"]
    target_hdfs_local_path = cluster_config["target_hdfs_local_path"]

    source_kinit_user = cluster_config["source_kinit_user"]
    source_kinit_kt = cluster_config["source_kinit_kt"]

    target_kinit_user = cluster_config["target_kinit_user"]
    target_kinit_kt = cluster_config["target_kinit_kt"]

    project_name = sa_config["project"]
    sa_version = float(sa_config["sa_version"])
    transfer_events_flag = program_config['transfer_events_flag'].lower()
    transfer_tag_flag = program_config['transfer_tag_flag'].lower()
    print_command_result = program_config['print_command_result'].lower()

    transfer_cross_environment = program_config['transfer_cross_environment'].lower()

    # 拼接要使用的数据库 database = f"etl_{project_name}"
    database = sa_config["database"]

    # 脚本执行 --历史数据的间隔周期
    # 间隔类型：day week month
    interval_type = program_config['interval_type']
    # 间隔时常
    interval_gap = program_config['interval_gap']

    # 每日执行前天数 - T+n
    before_day = program_config['before_day']

    # 日期
    date = ""

    def exec_command(self, command, command_description='', ssh_ip=None, status_exit=True):
        """
        调用执行 linux 命令
        :param command: 命令
        :param command_description: 命令描述
        :param ssh_ip: 遠程執行命令的電腦 ip
        :param status_exit: 如果命令執行錯誤是否需要退出當前程序
        :return:
        """
        command = command.strip()
        if ssh_ip:
            command = command.replace("'", "'\\''")
            command = f""" ssh {ssh_ip} '{command}' """
        log.write_log(f"------ command {command_description} ------ \n{command}\n")
        status, output = subprocess.getstatusoutput(command)
        if status != 0 and status_exit:
            self.write_command_wrong_recond(command)
            self.write_command_wrong_recond(output)
            log.deal_error(output)
            sys.exit(1)
        elif self.print_command_result == "true":
            log.write_log(f"------ result ------ \n {output}\n")
        return output

    def exec_command_retry(self, command, command_description='', ssh_ip=None, status_exit=True):
        """
        调用执行 linux 命令
        :param command: 命令
        :param command_description: 命令描述
        :param ssh_ip: 遠程執行命令的電腦 ip
        :param status_exit: 如果命令執行錯誤是否需要退出當前程序
        :return:
        """
        command = command.strip()
        if ssh_ip:
            command = command.replace("'", "'\\''")
            command = f""" ssh {ssh_ip} '{command}' """
        log.write_log(f"------ exec_command_retry {command_description} ------ \n{command}\n")
        times = 0
        while times < 3:
            status, output = subprocess.getstatusoutput(command)
            if status != 0 and status_exit:
                log.write_log(f"------ exec_command_retry ------ \n {output}\n")
                if times >= 3:
                    self.write_command_wrong_recond(command)
                    self.write_command_wrong_recond(output)
                    log.deal_error(output)
                    sys.exit(1)
            elif self.print_command_result == "true":
                log.write_log(f"------ exec_command_retry ------ \n {output}\n")
                return output
            times += 1

    def impala_query_sql(self, sql, command_description='', status_exit=True):
        sql = sql.replace("\n", "").replace("'", "'\\''").strip()
        command = f"""
            impala-shell -s {self.impala_kerberos_service_name} -i{self.impala_machine_host} -q '{sql}';
            """.strip()
        return self.exec_command(command, command_description=command_description, status_exit=status_exit)

    def hive_query_sql(self, sql, command_description='', status_exit=True):
        sql = sql.replace("\n", "").strip()
        command = f"""
        hive -e '{sql}';
        """.strip()
        return self.exec_command(command, command_description=command_description,
                                 ssh_ip="%s@%s" % (self.hive_trust_user, self.hive_machine_host),
                                 status_exit=status_exit)

    def date_to_timestamp(self, date) -> int:
        """
        字符串转毫秒级时间戳
        :rtype: object
        """
        time_array = time.strptime(date, date_format)
        time_stamp = int(time.mktime(time_array)) * 1000
        return time_stamp

    def decrypt(self, encrypted, key_name):
        try:
            with open(self.root_path + f"/data/{key_name}_key", "r", encoding="utf-8") as f:
                key_int = int(f.read())
                decrypted = int(encrypted) ^ key_int
                length = (decrypted.bit_length() + 7) // 8
                decrypted_bytes = int.to_bytes(decrypted, length, 'big')
            return decrypted_bytes.decode(encoding='utf-8', errors="ignore").strip()
        except Exception as e:
            log.write_log("如果沒有對密碼加密, 請先使用encrypt.py對\n密碼加密, 解密時發生錯誤，錯誤描述為：%s" % e)

    def get_date_flag(self):
        """
        斷點續傳功能, 查看是否有日期標識
        :return:
        """
        command = f"""
        hdfs dfs -cat {self.source_hdfs_cache_path}/{self.database}/date_flag/date_flag.txt
        """
        result = self.exec_command(command, command_description="get_date_flag", status_exit=False).strip()
        return result if result and ("No such file or directory" not in result) else ""

    def set_date_flag(self, current_date):
        local_file_path = os.path.join(self.root_path, "data/date_flag.txt")
        with open(local_file_path, "w") as f:
            f.write(current_date.strip())
        command = f"""
              hdfs dfs -put -f {local_file_path} {self.source_hdfs_cache_path}/{self.database}/date_flag
        """
        self.exec_command(command, "set_date_flag")

    # 获取每日导入的开始日期
    def read_date_start_flag(self):
        local_file_path = os.path.join(self.root_path, "data/date_start_flag.txt")
        for line in open(local_file_path):
             return line

    # 读取本地文件 获取历史导入的开始日期
    def read_date_flag_txt(self):
        local_file_path = os.path.join(self.root_path, "data/date_flag.txt")
        for line in open(local_file_path):
            return line

    # 把已导入的日期系如记录
    def write_exported_dates(self, date):
        local_file_path = os.path.join(self.root_path, "data/exported_dates.txt")
        with open(local_file_path, mode="a") as of:
            of.writelines(date)

    # 记录 commnd 执行错误记录
    def write_command_wrong_recond(self, line):
        data_path = "log/" + self.date + ".log"
        local_file_path = os.path.join(self.root_path, data_path)
        with open(local_file_path, mode="a") as of:
            of.writelines(line)

    def init_source_cluster(self):
        """
        初始化源 sa 集群的數據庫和 hdfs 路徑
        :rtype: object
        """
        # command = f"""
        # CREATE DATABASE IF NOT EXISTS {self.database};
        # """
        # self.impala_query_sql(command, "init_source_cluster")
        # 創建緩存文件路徑和日期標識路徑
        command = f"""
        hdfs dfs -test -d {self.source_hdfs_cache_path};
        echo $?;
        """.strip()
        ret_code = self.exec_command(command, "init_source_cluster")
        if ret_code == "1":
            command = f"""
            # export HADOOP_USER_NAME=hdfs;
            hdfs dfs -mkdir -p {self.source_hdfs_cache_path}/{self.database}/date_flag;
            hdfs dfs -mkdir -p {self.source_hdfs_cache_path}/{self.database}/tag;
            hdfs dfs -chmod -R 777 {self.source_hdfs_cache_path}/{self.database}/date_flag
            hdfs dfs -chmod -R 777 {self.source_hdfs_cache_path}
            hdfs dfs -chown -R sensors:sensors {self.source_hdfs_cache_path}
            """.strip()
            self.exec_command(command, "init_source_cluster")
        command = f"""
        kinit -k -t {self.source_kinit_kt} {self.source_kinit_user};
        """
        self.exec_command(command, "init_source_cluster")

    def init_target_cluster(self):
        """
        初始化目標集群的數據庫和 hdfs 路徑
        :rtype: object
        """
        # command = f"""
        # CREATE DATABASE IF NOT EXISTS {self.database};
        # """
        # self.hive_query_sql(command, "init_target_cluster")
        command = f"""
               hdfs dfs -test -d {self.target_hdfs_cache_path};
               echo $?;
               """.strip()
        ret_code = self.exec_command(command, "init_source_cluster",
                                     ssh_ip="%s@%s" % (self.hive_trust_user, self.hive_machine_host))
        if ret_code == "1":
            command = f"""
            # export HADOOP_USER_NAME=hdfs;
            hdfs dfs -mkdir -p {self.target_hdfs_cache_path}/{self.database}/tag;
            hdfs dfs -chmod -R 777 {self.target_hdfs_cache_path}
            """.strip()
            self.exec_command(command, "init_target_cluster",
                              ssh_ip="%s@%s" % (self.hive_trust_user, self.hive_machine_host))
        command = f"""
                kinit -k -t {self.target_kinit_kt} {self.target_kinit_user};
                """
        self.exec_command(command, "init_target_cluster",
                          ssh_ip="%s@%s" % (self.hive_trust_user, self.hive_machine_host))


class TagTransfer(BaseTransfer):
    def __init__(self):
        """
        转移 tag 数据的内部类
        :rtype: object
        """
        self.day = None
        # 标签数据的字段
        self.tag_table_name_list = list()
        # hash表
        self.tag_table_value_list = {}

        self.table_tag_list = []
        # 数据类型
        #data_type_list = ["number", "string", "list", "date", "datetime", "bool", "unknown"]
        data_type_list = ["double", "string", "string", "string", "bigint", "bigint", "string"]
        mysql_client = None
        try:
            mysql_client = MysqlClient(
                host=self.mysql_config["mysql_host"],
                port=self.mysql_config["mysql_port"],
                database=self.mysql_config["mysql_database"],
                user=self.mysql_config["mysql_user"],
                password=self.decrypt(self.mysql_config["mysql_password"], "mysql_password"),
            )
            mysql_client.cursor_obj.execute(f"select id from project where name = '{self.project_name}'")
            project_id = mysql_client.cursor_obj.fetchone()
            if not project_id:
                raise Exception(f"The project: {self.project_name} does not exist")
            project_id = project_id[0]
            sql = f"""
                    select t.id as id, t.name,name, p.status as partition_status
                    from sp_user_tag t, sp_user_tag_partition p 
                    where p.user_tag_id = t.id 
                    and t.tag_type = 1 
                    and t.project_id = {project_id}
                        group by t.id 
                    order by t.name
                    ;
                """
            mysql_client.cursor_obj.execute(sql)
            tag_datas = mysql_client.cursor_obj.fetchall()
            log.write_log(f"------ result tag ------ \n {tag_datas}\n")

            for i in tag_datas:
                if i[-1].lower() != "new":
                    tag_id = int(i[0])
                    value_sql = f"""
                        select data_type from sps_user_trait_property where tag_id = {tag_id}    
                        ;
                    """
                    self.tag_table_name_list.append(i[1])
                    for data_type in mysql_client.sql_query(value_sql):
                        self.tag_table_value_list[i[1]] = data_type_list[int(data_type["data_type"]) - 1]
        except Exception:
            traceback.print_exc()
        finally:
            if mysql_client is not None:
                mysql_client.close()

    def create_source_cluster_table(self):
        desc_tag_table_str = self.impala_query_sql(f"use {self.database}; show tables",
                              f"tagTransfer show source cluster {self.database} tables")
        self.table_tag_list = desc_tag_table_str.replace(" ", "").split("|")
        for tag_name in self.tag_table_name_list:
            log.write_log(f"------ result tag_name ------ \n {tag_name}\n")
            if not self.table_tag_list.__contains__("user_tag_" + tag_name):
                create_table_sql = f"""
                create table if not exists {self.database}.user_tag_{tag_name} (
                `user_id` bigint,
                `distinct_id` string,
                `value` {self.tag_table_value_list[tag_name]}
                )
                partitioned by (`base_time` bigint)
                LOCATION "hdfs://{self.source_hdfs_cache_path}/{self.database}/tag/user_tag_{tag_name}" ;
                """.strip()
                self.impala_query_sql(create_table_sql, "TagTransfer create_source_cluster_table")

    def insert_source_cluster_table(self):
        """
        sa 1.5的标签查询
        :return:
        """
        # 导入数据 , 如果是老版本的系统, 使用的是昨天的时间戳
        if self.sa_version > 1.15:
            base_time = self.date_to_timestamp(self.day)
        else:
            base_time = self.date_to_timestamp(self.day) - 86400000
        for tag_name in self.tag_table_name_list:
            insert_table_sql = f"""
            insert overwrite table {self.database}.user_tag_{tag_name}
             partition(`base_time`) /*SA_BEGIN({self.project_name})*/ select `user_id`, `distinct_id`, `value`, `base_time` from user_tag_{tag_name} where base_time = {base_time} /*SA_END*/;
            """.strip()
            self.impala_query_sql(insert_table_sql, "TagTransfer insert_source_cluster_table")

    def transfer_data(self):
        if self.transfer_cross_environment == "true":
            # 数据本地落盘
            command = f"""
                       hadoop fs -get {self.source_hdfs_cache_path}/{self.database}/tag  {self.source_hdfs_local_path}
                       """
            self.exec_command(command, "TagTransfer get_data_to_local_path")
            # ssh scp 文件传输到target cluster 指定文件目录
            command = f"""
                        scp -r {self.source_hdfs_local_path}/tag {self.target_ssh_trust_user}@{self.target_cluster_hdfs_host}:{self.target_hdfs_local_path}
                        """
            self.exec_command(command, "TagTransfer scp_data_to_target_cluster")
            # target cluster 存入导入文件
            command = f"""
                        hadoop fs -put -f  {self.target_hdfs_local_path}/tag  {self.target_hdfs_cache_path}/{self.database}
                        """
            self.exec_command_retry(command, "TagTransfer put_data_to_target_hdfs",
                                    ssh_ip="%s@%s" % (self.target_ssh_trust_user, self.target_cluster_hdfs_host))
            # 删除source磁盘数据
            command = f"""
                        rm -rf {self.source_hdfs_local_path}/tag/*
                     """
            self.exec_command(command, "EventsTransfer delete_date_from_source_local_path")

            # 删除target 磁盘数据
            command = f"""
                        rm -rf {self.target_hdfs_local_path}/tag/*            
                      """
            self.exec_command(command, "EventsTransfer delete_date_from_target_local_path",
                              ssh_ip="%s@%s" % (self.target_ssh_trust_user, self.target_cluster_hdfs_host))
        else:
            command = f"""
            hadoop distcp -update hdfs://{self.source_cluster_hdfs_host}:{self.source_cluster_hdfs_port}{self.source_hdfs_cache_path}/{self.database}/tag hdfs://{self.target_cluster_hdfs_host}:{self.target_cluster_hdfs_port}{self.target_hdfs_cache_path}/{self.database}/tag
            """
            self.exec_command(command, "TagTransfer transfer_data")

    def create_target_cluster_table(self):
        for tag_name in self.tag_table_name_list:
            if not self.table_tag_list.__contains__("user_tag_" + tag_name):
                create_table_sql = f"""
                    SET hive.create.as.external.legacy=true;
                    create table if not exists {self.database}.user_tag_{tag_name} (
                    `user_id` bigint,
                    `distinct_id` string,
                    `value` {self.tag_table_value_list[tag_name]}
                    )
                    partitioned by (`base_time` bigint)
                    LOCATION "hdfs://{self.target_hdfs_cache_path}/{self.database}/tag/user_tag_{tag_name}"
                    STORED AS TEXTFILE ;
                """
                self.hive_query_sql(create_table_sql, "TagTransfer create_target_cluster_table")

    def insert_target_cluster_table(self):
        for tag_name in self.tag_table_name_list:
            command = f"""
            msck repair table {self.database}.user_tag_{tag_name};
            """
            self.hive_query_sql(command, "TagTransfer insert_target_cluster_table")


class EventsTransfer(BaseTransfer):
    def __init__(self):
        """
        转移 events 数据的内部类
        :rtype: object
        """
        self.day = None
        # sa 数据底层使用的数据类型字典
        self.type_map = {
            "1": "bigint",
            "2": "string",
            "3": "string",
            "4": "bigint",
            "5": "bigint",
            "6": "bigint",
        }
        # events 表的字段字典
        self.field_dict_list = list()

        self.table_field_list = []
        mysql_client = None
        try:
            # 查询项目 id
            mysql_client = MysqlClient(
                host=self.mysql_config["mysql_host"],
                port=self.mysql_config["mysql_port"],
                database=self.mysql_config["mysql_database"],
                user=self.mysql_config["mysql_user"],
                password=self.decrypt(self.mysql_config["mysql_password"], "mysql_password"),
            )
            mysql_client.cursor_obj.execute(f"select id from project where name = '{self.project_name}'")
            project_id = mysql_client.cursor_obj.fetchone()
            if not project_id:
                raise Exception(f"The project: {self.project_name} does not exist")
            project_id = project_id[0]
            # table_type 字段的数据字典 EVENT(0), USER(1), ITEM(2), USER_TAG(3), UNKNOWN(-1);
            # data_type 字段数据字典 NUMBER(1), STRING(2), LIST(3), DATE(4), DATETIME(5), BOOL(6), UNKNOWN(-1);
            table_type = 0
            # 1.15 版本的 sa, 没有虚拟属性这个功能, 虚拟属性字段在 sp_pre_property_define 这张表
            sql = f"""
                select name, data_type from property_define where project_id = {project_id} and table_type = {table_type} and is_in_use = 1 and view_column_name is not NULL
                union all 
                select name, data_type from external_property_define where project_id = {project_id} and table_type = {table_type} and overwrite = 0
            """
            if self.sa_version > 1.15:
                sql += f""" union all select name, data_type from sp_pre_property_define where project_id = {project_id} and table_type = {table_type} and tracked = 0"""
            for i in mysql_client.sql_query(sql):
                self.field_dict_list.append(i)
            # user_id, distinct_id, time 字段在该表查询不到
            self.field_dict_list.extend([
                {
                    "name": "user_id",
                    "data_type": 1,
                },
                {
                    "name": "distinct_id",
                    "data_type": 2,
                },
                {
                    "name": "time",
                    "data_type": 5,
                },
            ])
        finally:
            if mysql_client is not None:
                mysql_client.close()

    def cast_type(self, field_name, type_index):
        if field_name == 'time':
            return "timestamp"
        return self.type_map[type_index]

    def create_source_cluster_table(self):
        # 创建 events 临时表
        # 使用 impala 创建 hive 的 events 表
        create_table_sql = f"""
            create table if not exists {self.database}.events  (
            {", ".join([f"`{i['name'].replace('$', 'dollar_')}` {self.cast_type(i['name'], str(i['data_type']))}" for i in self.field_dict_list])}
            )
            partitioned by (`date` string, `event` string)
            STORED AS PARQUET
            LOCATION "hdfs://{self.source_hdfs_cache_path}/{self.database}/events";
        """.strip().replace("\n", "")
        self.impala_query_sql(create_table_sql, "EventsTransfer create_source_cluster_table")
        # 判斷是否有新增的列, 從而需要 alter 表
        try:
            self.impala_query_sql(f"use {self.database}; desc events",
                                               f"first time EventsTransfer desc source cluster {self.database}.events")
        finally:
            desc_table_str = self.impala_query_sql(f"use {self.database}; desc events",
                                                   f"EventsTransfer desc source cluster {self.database}.events")
            self.table_field_list = desc_table_str.replace(" ","").split("|")
            for i in self.field_dict_list:
                # 底層表存的字段全部變成了小寫, 所以這裡需要先小寫在進行判斷
                field_name = i['name'].replace('$', 'dollar_').lower()
                field_type = self.cast_type(i['name'], str(i['data_type']))
                if not self.table_field_list.__contains__(field_name):
                    # 使用 alter 增加列
                    command = f"""ALTER TABLE {self.database}.events ADD COLUMNS({field_name} {field_type});"""
                    self.impala_query_sql(command, f"EventsTransfer alter source cluster table {self.database}.events",
                                          status_exit=False)

    def insert_source_cluster_table(self):
        # 查询 sa 的 events 表并导入数据
        insert_table_sql_meta_data = list()
        # sa events 表的元数据, list 类型需要把\n 分隔符替换掉, number 类型需要换成 bigint
        for meta_data in self.field_dict_list:
            if meta_data["data_type"] == 3:
                insert_table_sql_meta_data.append(
                    f"""replace({meta_data['name']}, chr(10), chr(44)) as {meta_data['name']} """)
            elif meta_data["data_type"] == 1:
                insert_table_sql_meta_data.append(f"cast({meta_data['name']} as bigint) as {meta_data['name']}")
            else:
                insert_table_sql_meta_data.append(meta_data["name"])
        insert_table_sql = f"""
        insert overwrite table {self.database}.events ({", ".join([f"{i['name'].replace('$', 'dollar_')}" for i in self.field_dict_list])})
         partition(`date`, `event`) /*SA_BEGIN({self.project_name})*/ select {", ".join(insert_table_sql_meta_data)}, to_date(`date`) as `date`,`event` from events where to_date(`date`) = '{self.day}' /*SA_END*/;
        """.strip()
        self.impala_query_sql(insert_table_sql, "EventsTransfer insert_source_cluster_table")

    def transfer_data(self):
        if self.transfer_cross_environment == "true":
            # source 数据本地落盘
            command = f"""
                       hadoop fs -get {self.source_hdfs_cache_path}/{self.database}/events  {self.source_hdfs_local_path}
                       """
            self.exec_command(command, "EventsTransfer get_data_to_local_path")
            # ssh scp 文件传输到target cluster 指定文件目录
            command = f"""
                        scp -r {self.source_hdfs_local_path}/events {self.target_ssh_trust_user}@{self.target_cluster_hdfs_host}:{self.target_hdfs_local_path}
                        """
            self.exec_command(command, "EventsTransfer scp_data_to_target_cluster")
            # target cluster 存入导入文件
            command = f"""
                        hadoop fs -put -f  {self.target_hdfs_local_path}/events  {self.target_hdfs_cache_path}/{self.database}
                        """
            self.exec_command_retry(command, "EventsTransfer put_data_to_target_hdfs",
                                    ssh_ip="%s@%s" % (self.target_ssh_trust_user, self.target_cluster_hdfs_host))
            # 删除source磁盘数据
            command = f"""
                        rm -rf {self.source_hdfs_local_path}/events/*
                        """
            self.exec_command(command, "EventsTransfer delete_date_from_source_local_path")
            # 删除target 磁盘数据
            command = f"""
                        rm -rf {self.target_hdfs_local_path}/events/*            
                    """
            self.exec_command(command, "EventsTransfer delete_date_from_target_local_path",
                              ssh_ip="%s@%s" % (self.hive_trust_user, self.target_cluster_hdfs_host))
        else:
            command = f"""
            hadoop distcp -update hdfs://{self.source_cluster_hdfs_host}:{self.source_cluster_hdfs_port}/{self.source_hdfs_cache_path}/{self.database}/events hdfs://{self.target_cluster_hdfs_host}:{self.target_cluster_hdfs_port}/{self.target_hdfs_cache_path}/{self.database}/events
            """
            self.exec_command(command, "EventsTransfer transfer_data")

    def create_target_cluster_table(self):
        create_table_sql = f"""
        SET hive.create.as.external.legacy=true;
        create table if not exists {self.database}.events (
        {", ".join([f"`{i['name'].replace('$', 'dollar_')}` {self.cast_type(i['name'], str(i['data_type']))}" for i in self.field_dict_list])}
        )
        partitioned by (`date` string, `event` string)
        STORED AS PARQUET
        LOCATION "hdfs://{self.target_hdfs_cache_path}/{self.database}/events" ;
        """.strip()
        self.hive_query_sql(create_table_sql, "EventsTransfer create_target_cluster_table")
        # 判斷是否有新增的列, 從而需要 alter 表
        try:
            self.hive_query_sql(f"desc {self.database}.events",
                                                 f"first time EventsTransfer desc target cluster {self.database}.events")
        finally:
            for i in self.field_dict_list:
                field_name = i['name'].replace('$', 'dollar_').lower()
                field_type = self.cast_type(i['name'], str(i['data_type']))
                if not self.table_field_list.__contains__(field_name):
                    command = f"""ALTER TABLE {self.database}.events ADD COLUMNS({field_name} {field_type});"""
                    self.hive_query_sql(command, f"EventsTransfer alter table target cluster {self.database}.events",
                                        status_exit=False)

    def insert_target_cluster_table(self):
        command = f"""
        msck repair table {self.database}.events;
        """
        self.hive_query_sql(command, "EventsTransfer insert_target_cluster_table")


def process_transfer(begin_time: datetime.date, end_time: datetime.date, transfer_type):
    base_transfer = BaseTransfer()
    events_transfer = EventsTransfer()
    tag_transfer = TagTransfer()
    # 初始化源集群
    base_transfer.init_source_cluster()
    # 初始化目標集群
    base_transfer.init_target_cluster()
    if base_transfer.transfer_events_flag == "true":
        events_transfer.create_source_cluster_table()
        events_transfer.create_target_cluster_table()
    if base_transfer.transfer_tag_flag == "true":
        tag_transfer.create_source_cluster_table()
        tag_transfer.create_target_cluster_table()
    # 导入历史数据
    days = 0
    if transfer_type == "history":
        # 斷點續傳, 先判斷是否有之前已經日期標識
        date_flag = base_transfer.get_date_flag()
        if date_flag == "":
            date_flag = base_transfer.read_date_flag_txt().strip()
        begin_time = datetime.datetime.strptime(date_flag, date_format) if date_flag else begin_time
        interval = {"day": 1, "week": 7, "month": 30}
        days = interval.get(base_transfer.interval_type) * int(base_transfer.interval_gap)
        history_end_time = (begin_time + datetime.timedelta(days=days)).strftime(date_format)
        hed = datetime.datetime.strptime(history_end_time, date_format)
        date_start_flag_txt = base_transfer.read_date_start_flag().strip()
        date_start_flag = datetime.datetime.strptime(date_start_flag_txt, date_format)
        end_time = date_start_flag if hed > date_start_flag else hed
        betweendays = 0
        betweendays = (end_time - begin_time).days
        if betweendays <= 0:
            return
        else:
            days = days if days < betweendays else betweendays
    # 轉移數據
    days = days if transfer_type == "history" else (end_time - begin_time).days + 1
    for i in range(days):
        n = i
        if transfer_type == "history":
            n = i + 1
        elif transfer_type == "everyday":
            n = -int(base_transfer.before_day)
        events_transfer.day = tag_transfer.day = current_day = (begin_time + datetime.timedelta(days=n)).strftime(
            date_format)
        log.write_log(f"----- process_transfer date: {current_day} -----")
        transfer_data = ""
        base_transfer.date = datetime.datetime.strptime(current_day, date_format)
        # 轉移 events 數據
        if base_transfer.transfer_events_flag == "true":
            events_transfer.insert_source_cluster_table()
            events_transfer.transfer_data()
            events_transfer.insert_target_cluster_table()
            transfer_data = "events"
        # 轉移 tag 數據
        if base_transfer.transfer_tag_flag == "true":
            tag_transfer.insert_source_cluster_table()
            tag_transfer.transfer_data()
            tag_transfer.insert_target_cluster_table()
            transfer_data = transfer_data + " + tag"
        # 已迁移日期记录
        base_transfer.write_exported_dates(transfer_data + "：" + current_day)
        # 历史数据迁移 - 設置 date 標識
        if transfer_type == "history":
            base_transfer.set_date_flag(current_day)


def run(args_):
    try:
        begin_time = datetime.datetime.strptime(args_.begin_time, date_format) \
            if args_.begin_time else (datetime.datetime.now() + datetime.timedelta(days=0)).strftime(date_format)
        end_time = datetime.datetime.strptime(args_.end_time, date_format) \
            if args_.end_time else (datetime.datetime.now() + datetime.timedelta(days=0)).strftime(date_format)
        if isinstance(begin_time, str):
            bt = datetime.datetime.strptime(begin_time, date_format)
            begin_time = bt
        if isinstance(end_time, str):
            et = datetime.datetime.strptime(end_time, date_format)
            end_time = et
        # 数据导入类型
        transfer_type = args_.transfer_type
        if transfer_type == "":
            transfer_type = args_.transfer_type if args_.begin_time else "everyday"
        if begin_time > end_time:
            raise Exception(
                f"end_time: {end_time.strftime(date_format)} must be greater than begin_time: {begin_time.strftime(date_format)}")
        process_transfer(begin_time, end_time, transfer_type)
    except Exception:
        log.write_log(f"錯誤發生：{traceback.format_exc()}")
    finally:
        log.close()


if __name__ == '__main__':
    date_format = "%Y-%m-%d"
    build_parameters = {
        "run": {
            "help": "數據遷移程序",
            "defaults_func": run,
            "arguments": [
                {
                    "args": ['-b', '--begin_time'],
                    "kwargs": {
                        "type": str,
                        "help": "數據起始時間, 如: 2020-01-01(包括當天時間)",
                        "default": "",
                    }
                },
                {
                    "args": ['-e', '--end_time'],
                    "kwargs": {
                        "type": str,
                        "help": "數據結束時間, 如: 2020-01-01(包括當天時間)",
                        "default": "",
                    }
                },
                {
                    "args": ['-t', '--transfer_type'],
                    "kwargs": {
                        "type": str,
                        "help": "脚本执行类型, 如: history表示迁移历史数据 / everyday 表示迁移每日数据",
                        "default": "",
                    }
                }
            ],
        },
    }
    parsers = argparse.ArgumentParser()
    subparsers = parsers.add_subparsers(help="tag_transfer")
    for k in build_parameters.keys():
        parsers_k = subparsers.add_parser(k, help=build_parameters[k]["help"])
        for argument in build_parameters[k].get("arguments", list()):
            parsers_k.add_argument(*argument["args"], **argument["kwargs"])
        if build_parameters[k].get("defaults_func"):
            parsers_k.set_defaults(func=build_parameters[k]["defaults_func"])
    args = parsers.parse_args()
    args.func(args)
