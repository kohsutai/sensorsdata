import argparse
import json
import os
import shutil
import sys
import traceback
from itertools import islice

import requests

sys.path.append(os.path.abspath(os.path.abspath('..')))

from datetime import timedelta
import datetime
from traceback import format_exc

import dateutil.parser
import pytz
import myUtils
import log
import re


class TagExport:
    def __init__(self, args_):
        self.args = args_
        self._tz = pytz.timezone('Asia/Shanghai')
        # 主目錄
        self.root_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../")
        config_file_path = os.path.join(self.root_path, "config/tag_export_conf.conf")
        self.tag_config = myUtils.get_config(section="tag_export", config_path=config_file_path)
        self.sqlserver_config = myUtils.get_config(section="sqlserver", config_path=config_file_path)
        self.mysql_config = myUtils.get_config(section="mysql", config_path=config_file_path)
        self.ftp_config = myUtils.get_config(section="ftp", config_path=config_file_path)
        self.log_config = myUtils.get_config(section="log", config_path=config_file_path)
        self.file_name_config = myUtils.get_config(section="file_name", config_path=config_file_path)
        self.log = log.Log()

        # 中轉文件的存儲路徑
        self.temp_path = myUtils.check_ends_with(self.tag_config["data_dir"])
        if self.args.base_time.strip():
            # 標籤導出原始 csv 文件路徑
            self.data_path = f"{self.temp_path}data/{self.args.base_time.strip().replace('-', '')}/"
            # 標籤導出整合的 csv 文件路徑
            self.today_csv_file = f"{self.data_path}tag.{self.args.base_time.strip().replace('-', '')}.csv"
            # 資料檔文件和控制當文件
            yesterday = self.get_yesterday_str(current_day=self.args.base_time.strip(), current_format="%Y-%m-%d",
                                               yesterday_format="%Y%m%d")
            self.control_file_name = f"{self.file_name_config['sys_code']}_{self.file_name_config['table']}_{yesterday}.H".upper()
            self.data_file_name = f"{self.file_name_config['sys_code']}_{self.file_name_config['table']}_{yesterday}.D".upper()

            # 查询 mysql 指定的 base_time
            self.base_time = self.get_yesterday_str(self.args.base_time.strip())
        else:
            # 標籤導出原始 csv 文件路徑
            self.data_path = f"{self.temp_path}data/{self.future_raw(0)}/"
            # 標籤導出整合的 csv 文件路徑
            self.today_csv_file = f"{self.data_path}tag.{self.future_raw(0)}.csv"
            # 資料檔文件和控制當文件
            self.control_file_name = f"{self.file_name_config['sys_code']}_{self.file_name_config['table']}_{self.future_raw(-1)}.H".upper()
            self.data_file_name = f"{self.file_name_config['sys_code']}_{self.file_name_config['table']}_{self.future_raw(-1)}.D".upper()

            # 查询 mysql 指定的 base_time
            self.base_time = self.future_raw(-1, date_format="%Y-%m-%d")

        self.control_file = f"{self.data_path}{self.control_file_name}"
        self.data_file = f"{self.data_path}{self.data_file_name}"

        # 初始化 mysql 客戶端
        self.mysql_client = myUtils.MysqlClient(host=self.mysql_config["mysql_host"],
                                                port=self.mysql_config["mysql_port"],
                                                database=self.mysql_config["mysql_database"],
                                                user=self.mysql_config["mysql_user"],
                                                password=self.decrypt(self.mysql_config["mysql_password"],
                                                                      "mysql_password"))

        # 初始化 ftp 客户端
        self.ftp_client = None

    def get_yesterday_str(self, current_day, current_format="%Y-%m-%d", yesterday_format="%Y-%m-%d"):
        """
        1.2版本之前的标签, 当前计算的数据, base_time 是昨天的零点, 所以需要获取昨天的时间
        :rtype: object
        """
        return (datetime.datetime.strptime(current_day, current_format) + datetime.timedelta(days=-1)).strftime(
            yesterday_format)

    def decrypt(self, encrypted, key_name):
        try:
            with open(self.root_path + f"/data/{key_name}_key", "r", encoding="utf-8") as f:
                key_int = int(f.read())
                decrypted = int(encrypted) ^ key_int
                length = (decrypted.bit_length() + 7) // 8
                decrypted_bytes = int.to_bytes(decrypted, length, 'big')
            return decrypted_bytes.decode(encoding='utf-8', errors="ignore").strip()
        except Exception as e:
            self.log.write_log("如果沒有對密碼加密, 請先使用encrypt.py對\n密碼加密, 解密時發生錯誤，錯誤描述為：%s" % e)

    def future_raw(self, x, today=None, date_format='%Y%m%d'):
        if not today:
            return dateutil.parser.parse(
                (datetime.datetime.now(self._tz) + timedelta(days=x)).isoformat()
            ).strftime(date_format)
        else:
            return (today + timedelta(days=x)).strftime(date_format)

    def init_dir(self):
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

    def export_tag_data(self, tag_name, base_time, profile_field_list=None, export_file_name=""):
        """
        調用 sa 的 api 接口獲取標籤數據
        :rtype: object
        """
        try:
            # 查询 admin 的密码
            self.mysql_client.cursor_obj.execute(
                "select password from user where username = 'admin' and is_global = 1;")
            token = self.mysql_client.cursor_obj.fetchone()[0]
            params = {
                'token': token,
                'project': self.tag_config["project"],
            }
            data = {
                "filter": {
                    "conditions": [
                        {
                            "field": f"user.{tag_name}@{base_time} 00:00:00",
                            "function": "isSet"
                        }
                    ]
                },
                "all_page": True,
                "detail": True,
                "profiles": [f"user.{tag_name}"]
            }
            if profile_field_list:
                # 用户属性前面要加 "user."
                data["profiles"].extend(list(map(lambda x: f"user.{x}", profile_field_list)))
            headers = {
                'Content-Type': 'application/json'
            }
            r = requests.post(url=self.tag_config["uri"], params=params, data=json.dumps(data), headers=headers)
            if r.status_code == 200:
                export_file_name = export_file_name or tag_name
                export_file_path = f"{self.data_path}{export_file_name}.csv"
                with open(export_file_path, "wb") as f:
                    f.write(r.content)
                data_line_num = int(os.popen(f"cat {export_file_path} | wc -l ").read().strip())
                if data_line_num > 1:
                    self.log.write_log(
                        f"tag_name: {tag_name}, base_time: {base_time} 00:00:00, 標籤導出成功!, 數據量: {data_line_num}")
                    return True
                else:
                    self.log.write_log(
                        f"tag_name: {tag_name}, base_time: {base_time} 00:00:00, 標籤導出成功, 但數據量有問題, 數據量: {data_line_num}")
            else:
                self.log.write_log(f"tag_name: {tag_name}, base_time: {base_time} 00:00:00, 標籤導出失敗!, 請求返回值: {r.text}")
        except Exception:
            self.log.write_log(
                f"tag_name: {tag_name}, base_time: {base_time} 00:00:00 export_tag_data 方法出錯\n{traceback.format_exc()}")

    def sink_csv(self):
        """
        導出標籤數據, 保存為一個 csv 文件
        :rtype: object
        """
        # 判斷是否有備註字段, 如果有一定是屬於 users 表的字段
        memo_field = list(filter(lambda x: True if x else False,
                                 map(lambda x: x.strip(),
                                     [self.tag_config["memo_1"], self.tag_config["memo_2"],
                                      self.tag_config["memo_3"]])))
        # 先查出项目 id
        project = self.tag_config["project"]
        self.mysql_client.cursor_obj.execute(f"select id from project where name = '{project}'")
        project_id = self.mysql_client.cursor_obj.fetchone()[0]

        # 查詢項目 users 表的中英文字段映射關係
        name_cname_map = dict()
        for i in self.mysql_client.sql_query(
                f"SELECT name, cname from property_define where project_id = {project_id} and table_type = 1"):
            name_cname_map[i["name"]] = i["cname"]

        # 查询項目所擁有的標籤
        sql = f"""select t.name, t.cname, t.data_type, p.base_time
                    from sp_user_tag t, sp_user_tag_partition p 
                   where p.user_tag_id = t.id 
                     and t.tag_type = 1 
                     and t.project_id = {project_id} 
                     and p.base_time = '{self.base_time}'
         """
        tag_table_list = [i for i in self.mysql_client.sql_query(sql)]
        self.log.write_log(f"{project}項目下所有標籤: {tag_table_list}")
        # 如果指定了需要導出的標籤, 則篩選指定的標籤, 不然導出該項目下所有的標籤
        if self.tag_config["tag"].strip() and self.tag_config["tag"].strip().lower() != "all":
            specific_tag = list()
            tag_list = map(lambda x: x.strip(), self.tag_config["tag"].strip().split(","))
            for i in tag_list:
                flag = True
                for tag_table in tag_table_list:
                    if i == tag_table["name"]:
                        specific_tag.append(tag_table)
                        flag = False
                        break
                if flag:
                    self.log.write_log(f"標籤{i}不存在")
                    # raise Exception(f"標籤{i}不存在")
            tag_table_list = specific_tag
        # 如果已存在今天的 csv 文件, 需要删除
        if os.path.exists(self.today_csv_file):
            os.remove(self.today_csv_file)
        for i in tag_table_list:
            tag_name = i["name"]
            tag_cname = i["cname"]
            base_time = i["base_time"].strftime("%Y-%m-%d")
            tag_data_type = i["data_type"]

            # 導出數據的 csv 文件
            self.export_tag_data(tag_name, base_time, profile_field_list=memo_field)

            # 处理 csv
            # 轉換 csv 字段名為客戶要求的
            need_field = ["匿名 ID/登_ ID", "分群類別"]
            rename_map = {"登录 ID": "匿名 ID/登_ ID"}
            for index, field in enumerate(memo_field):
                rename_map[name_cname_map[field]] = "備註" + str(index + 1)
                need_field.append("備註" + str(index + 1))
            rename_map[tag_cname] = "分群類別"
            # 讀取 csv 文件生成 dataframe
            tag_df = myUtils.create_dataframe(f"{self.data_path}{tag_name}.csv", rename_map=rename_map)
            if tag_df is None:
                continue
            # 只獲取需要的字段
            tag_df = tag_df[need_field]
            # 刪除 匿名 ID/登_ ID 不存在的字段
            tag_df = tag_df.dropna(subset=['匿名 ID/登_ ID'])
            # 添加源 csv 文件不存在的字段, 比如 執行日期, 執行時間, 幾個沒有指定的備註字段
            tag_df["執行日期"] = i["base_time"].strftime("%Y-%m-%d")
            # tag_df["執行時間"] = i["base_time"].strftime("%H:%M")
            tag_df["執行時間"] = datetime.datetime.now().strftime("%H:%M")
            # 找出沒有賦值的備註字段, 並將該字段值賦值為空
            for index, field in enumerate(
                    [self.tag_config["memo_1"], self.tag_config["memo_2"], self.tag_config["memo_3"]]):
                if not field:
                    tag_df["備註" + str(index + 1)] = ""
            # 如果 data_type 是 6 , 说明该标签 value 是 bool 型, 需要把导出的值从 1 换成 cname
            #if tag_data_type == 6:
            tag_df["分群類別"] = tag_cname
            # 指定列顺序
            tag_df = tag_df[["執行日期", "執行時間", "匿名 ID/登_ ID", "分群類別", "備註1", "備註2", "備註3"]]
            # 合并 csv
            if not os.path.exists(self.today_csv_file):
                tag_df.to_csv(self.today_csv_file, index=False)
            else:
                tag_df.to_csv(self.today_csv_file, mode="a", header=False, index=False)

    def create_control_file(self, field_name_d, output_path_d, output_path_h):
        """
        生成控制檔文件
        :rtype: object
        """
        yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        # 资料起始日期
        start_time = yesterday
        # 资料结束日期
        end_time = yesterday
        # 输出文档时间
        export_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 获取输出文档的行数
        count = -1
        for count, line in enumerate(open(output_path_d, 'r')):
            pass
        count += 1
        line = str(start_time) + str(end_time) + field_name_d.ljust(64, " ") + str(export_time) + str(count).rjust(10,
                                                                                                                   '0')
        with open(output_path_h, "w+", encoding="utf-8") as f:
            f.write(line)
        self.log.write_log("生成控制檔(.H)")

    def create_data_file(self, input_path, output_path):
        """
        生成資料檔文件
        :rtype: object
        """
        self.log.write_log("開始處理數據")
        input_file = open(input_path, "r", encoding="utf-8")
        output_file = open(output_path, "w+", encoding="utf-8")
        try:
            next(input_file)
            for row in input_file:
                row = row.replace("|", "^")
                # item_list = row.split(",")
                item_list = re.split(''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''',row)
                for i in range(len(item_list)):
                    if len(item_list[i]) == 0 or item_list[i].isspace():
                        item_list[i] = "NULL"
                row = "|".join(item_list)
                if (row[-1:] != "\n"):
                    row += "\n"
                output_file.write(row)
            self.log.write_log("數據處理完畢，產出資料檔(.D)")
        except Exception as e:
            self.log.write_log("數據處理過程出錯，錯誤描述為：%s" % e)
        finally:
            input_file.close()
            output_file.close()

    def sink_ftp(self):
        """
        上傳.D, .H 文件到 ftp
        :rtype: object
        """
        # 初始化 ftp 客户端
        self.ftp_client = myUtils.FTPClient(host=self.ftp_config["ftp_host"], port=self.ftp_config["ftp_port"],
                                            user=self.ftp_config["ftp_username"],
                                            passwd=self.decrypt(self.ftp_config["ftp_password"], "ftp_password"),
                                            pasv=self.ftp_config["ftp_pasv"])
        ftp_path = myUtils.check_ends_with(self.ftp_config["ftp_path"])

        # 生产.D, .H 文件
        self.create_data_file(input_path=self.today_csv_file, output_path=self.data_file)
        self.create_control_file(field_name_d=self.data_file_name, output_path_d=self.data_file,
                                 output_path_h=self.control_file)

        # 上传文件到 ftp
        self.ftp_client.create_remote_path(ftp_path)
        self.ftp_client.upload_file(local_path=self.data_file, remote_path=ftp_path + self.data_file_name)
        self.ftp_client.upload_file(local_path=self.control_file, remote_path=ftp_path + self.control_file_name)

    def sink_sqlserver(self, batch=500):
        """
        上傳數據到 sqlserver
        :rtype: object
        """
        password = self.decrypt(self.sqlserver_config["sqlserver_password"], "sqlserver_password")
        sql_server_client = myUtils.SqlServerClient(host=self.sqlserver_config["sqlserver_host"],
                                                    port=self.sqlserver_config["sqlserver_port"],
                                                    database=self.sqlserver_config["sqlserver_database"],
                                                    user=self.sqlserver_config["sqlserver_user"],
                                                    password=password)
        table = self.sqlserver_config["sqlserver_table"]
        # 删除当天的数据, 方便重跑的时候数据不重复
        sql = f" DELETE FROM {table} WHERE date = '{self.base_time}'; "
        self.log.write_log(f"執行刪除語句 {sql}")
        sql_server_client.delete(sql)
        # 导入数据
        sql = f"INSERT INTO {table} (date, time, distinct_id, user_tag, memo_1, memo_2, memo_3) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        with open(self.today_csv_file) as f:
            header_data = [field.strip() for field in list(islice(f, 1))[0].strip().split(",")]
            while True:
                # 批量写入 sql 语句
                raw_data = list(islice(f, batch))
                line_data = list()
                for raw_line in raw_data:
                    raw_list = map(lambda x: x.strip(), raw_line.split(","))
                    line_data.append(dict(zip(header_data, raw_list)))
                val = tuple((i["執行日期"], i["執行時間"], i["匿名 ID/登_ ID"], i["分群類別"], i["備註1"], i["備註2"], i["備註3"]) for i in
                            line_data)
                sql_server_client.batch_insert(sql, val)
                if len(line_data) < batch:
                    break
        sql_server_client.close()

    def clean(self):
        """
        清除舊數據
        :rtype: object
        """
        # 清除本地緩存數據
        if self.tag_config["clean_interval"]:
            old = self.future_raw(0 - int(self.tag_config["clean_interval"]))
            old_csv_dir = f"{self.temp_path}data/{old}"
            # 清除文件夹
            if os.path.exists(old_csv_dir):
                shutil.rmtree(old_csv_dir)
        # 清除 ftp 文件
        if self.ftp_client and self.ftp_config["ftp_clean_interval"]:
            ftp_path = myUtils.check_ends_with(self.ftp_config["ftp_path"])
            old = self.future_raw(0 - int(self.ftp_config["ftp_clean_interval"]))
            old_control_file_name = f"{self.file_name_config['sys_code']}_{self.file_name_config['table']}_{old}.D".upper()
            old_data_file_name = f"{self.file_name_config['sys_code']}_{self.file_name_config['table']}_{old}.H".upper()
            self.ftp_client.del_remote_file(ftp_path, old_control_file_name)
            self.ftp_client.del_remote_file(ftp_path, old_data_file_name)

    def run(self):
        try:
            # 如果是重跑, 需要把之前跑過的數據也一併清理了, 以免數據重複
            if os.path.exists(self.data_path):
                shutil.rmtree(self.data_path)
            self.log.write_log("初始化 data 目錄……")
            self.init_dir()
            self.log.write_log("開始導出 csv 數據……")
            self.sink_csv()
        except Exception:
            self.log.write_log(f"錯誤發生：{format_exc()}")
            sys.exit(1)
        try:
            if self.ftp_config["ftp_valid_flag"].lower() == "true":
                self.log.write_log("開始上傳數據至 ftp……")
                self.sink_ftp()
        except Exception:
            self.log.write_log(f"錯誤發生：{format_exc()}")
        finally:
            if self.ftp_client:
                self.ftp_client.close()
        try:
            if self.sqlserver_config["sqlserver_valid_flag"].lower() == "true":
                self.log.write_log("開始上傳數據至 sqlserver……")
                self.sink_sqlserver()
        except Exception:
            self.log.write_log(f"錯誤發生：{format_exc()}")
            sys.exit(1)
        finally:
            if self.mysql_client:
                self.mysql_client.close()
            self.log.write_log("清理之前的数据!")
            self.clean()
            self.log.write_log("處理完成!")
            self.log.close()


def tag_run(args_):
    TagExport(args_).run()


if __name__ == '__main__':
    build_parameters = {
        "run": {
            "help": "運行標籤導出程序",
            "defaults_func": tag_run,
            "arguments": [
                {
                    "args": ['-b', '--base_time'],
                    "kwargs": {
                        "type": str,
                        "help": "選擇標籤導出的基準時間, 如: 2020-01-01",
                        "default": "",
                    }
                },
            ],
        },
    }
    parsers = argparse.ArgumentParser()
    subparsers = parsers.add_subparsers(help="tag_export")
    for k in build_parameters.keys():
        parsers_k = subparsers.add_parser(k, help=build_parameters[k]["help"])
        for argument in build_parameters[k].get("arguments", list()):
            parsers_k.add_argument(*argument["args"], **argument["kwargs"])
        if build_parameters[k].get("defaults_func"):
            parsers_k.set_defaults(func=build_parameters[k]["defaults_func"])
    args = parsers.parse_args()
    args.func(args)
