# -*- coding: utf-8 -*-
# @Time    : 2021/12/4 20:05
# @Author  : wenlongy
# @Email   : 247540299@qq.com
# @File    : my_slave_find.py

import pymysql
import argparse

db_user='ywl'
db_password='nihao123'
print_dict={
    "read_only": 'read_only',
    "rpl_semi_sync_master_enabled": 'semi_master',
    "rpl_semi_sync_slave_enabled": 'semi_slave',
    "Slave_IO_Running": 'IO_Run',
    "Slave_SQL_Running": 'SQL_Run',
    "Seconds_Behind_Master": 'Behind',
    "SQL_Delay": 'SQL_Delay',

}

global master_uuid
master_uuid=''
co_master_uuids = []
hosts_info={}
db_slave_port_range=[3000,10000]

def myconnect(host='',port=3306,user=db_user,password=db_password,connect_timeout=1):
    try:
        conn = pymysql.connect(host=host,port=port,user='ywl',password='nihao123',connect_timeout=1)
        # self.cursor = self.conn.cursor()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        return cursor
    except Exception as e:
        print(host,port,e)
        return False
        # print(self.cursor)

class GetHostInfo(object):
    def __init__(self,host='',port=3306):
        self.db_info={}
        self.host=host
        self.port=port
        self.db_info['host']=host
        self.db_info['port']=port
        self.get_variables_sql = """select * from  performance_schema.global_variables where 
         VARIABLE_NAME in ('rpl_semi_sync_master_enabled','rpl_semi_sync_slave_enabled','read_only','server_id','server_uuid');"""
        self.show_slave_status_sql = "show slave status"
        self.show_slave_hosts_sql = "show slave hosts"
        self.get_server_uuid_sql = "select @@server_uuid server_uuid"
        self.get_slave_hosts_sql = """select substring_index(host,':',1) host from information_schema.processlist 
        where command in ('Binlog Dump GTID','Binlog Dump');"""
        self.slave_port_list=[]
        self.slave_host_list=[]

    def get_variables(self):
        self.cursor.execute(self.get_variables_sql)
        variables = self.cursor.fetchall()
        for variable in variables:
            self.db_info[variable['VARIABLE_NAME']]=variable['VARIABLE_VALUE']

    def get_master_info(self):
        self.cursor.execute(self.show_slave_status_sql)
        slave_infos = self.cursor.fetchall()
        if not slave_infos:
            self.db_info['Master_Server_Id'] = ''
            self.db_info['Master_UUID'] = ''
        else:
            self.db_info['Master_Server_Id']=slave_infos[0]['Master_Server_Id']
            self.db_info['Master_UUID']=slave_infos[0]['Master_UUID']
            self.db_info['Slave_IO_Running']=slave_infos[0]['Slave_IO_Running']
            self.db_info['Slave_SQL_Running']=slave_infos[0]['Slave_SQL_Running']
            self.db_info['Seconds_Behind_Master']=slave_infos[0]['Seconds_Behind_Master']
            self.db_info['Master_Host']=slave_infos[0]['Master_Host']
            self.db_info['Master_Port']=slave_infos[0]['Master_Port']
            self.db_info['Connect_Retry']=slave_infos[0]['Connect_Retry']
            self.db_info['SQL_Delay']=slave_infos[0]['SQL_Delay']

    def get_slave_port(self):
        self.cursor.execute(self.show_slave_hosts_sql)
        show_slave_info = self.cursor.fetchall()
        return show_slave_info
        # for slave_host in show_slave_hosts:
        #     print(slave_host)
        #     slave_uuid_lists.append(slave_host['Slave_UUID'])
        #     slave_server_id_lists.append(slave_host['Server_id'])
        #     slave_port_list.append(slave_host['Port'])
        # return slave_port_list
    def get_slave_hosts(self):
        self.db_info['slave_host_list'] = {}
        show_slave_info=self.get_slave_port()
        if not show_slave_info:
            return
        self.cursor.execute(self.get_slave_hosts_sql)
        self.cursor.close()
        slave_hosts = self.cursor.fetchall()
        self.cursor.close()
        for slave_host in slave_hosts:
            for slave in show_slave_info:
                port = slave['Port']
                if port <db_slave_port_range[0] or port >db_slave_port_range[1]:
                    continue
                cursor=myconnect(host=slave_host['host'],port=port)

                if not cursor:
                    continue
                ret=cursor.execute(self.get_server_uuid_sql)
                if not ret:
                    continue
                get_server_uuid = cursor.fetchall()[0]['server_uuid']
                slave['Host']=slave_host['host']

                if get_server_uuid!=slave['Slave_UUID']:
                    continue
                self.db_info['slave_host_list'][get_server_uuid] = slave
                cursor.close()

    def get_host_info(self):
        self.cursor=myconnect(host=self.host,port=self.port)
        if self.cursor:
            self.get_variables()
            self.get_master_info()
            self.get_slave_hosts()
            hosts_info[self.db_info['server_uuid']]=self.db_info
            return self.db_info



def get_hosts_info(host='',port=3306):
    global master_uuid
    db_i = GetHostInfo(host=host, port=port)
    db_info=db_i.get_host_info()
    if db_info:
        if not db_info['Master_UUID']:
            master_uuid=db_i.db_info['server_uuid']
            # master_uuid.append(db_i.db_info['server_uuid'])
        elif db_info['Master_UUID'] in db_info['slave_host_list']:
            co_master_uuids.append(db_info['server_uuid'])
            # co_master_uuids.append(db_info['Master_UUID'])
        if db_info['slave_host_list']:
            for k,v in db_info['slave_host_list'].items():
                if k not in hosts_info:
                    get_hosts_info(host=v['Host'], port=v['Port'])
        if db_info['Master_UUID'] and db_info['Master_UUID'] not in hosts_info:
            get_hosts_info(host=db_info['Master_Host'], port=db_info['Master_Port'])


def print_topo(server_uuid='',i=0):
    db_info = { print_dict[k]:v for k,v in hosts_info[server_uuid].items() if k in print_dict}
    v= [ str(db_info[k])+' '*(len(k)-len(str(db_info[k]))+4) for k in db_info]
    # print(xxx)
    # print(v)

    if server_uuid in co_master_uuids  and len(co_master_uuids)==2:
        hv = "%s%s %s" % ('_' * i,
                          hosts_info[server_uuid]['host'] + '_' + str(hosts_info[server_uuid]['port']) + ' ' * (
                                      33 - i - len(hosts_info[server_uuid]['host'])),
                          ''.join(v)
                          )
        print('co:'+hv)
    else:
        hv = "%s%s %s" % ('_' * i,
                          hosts_info[server_uuid]['host'] + '_' + str(hosts_info[server_uuid]['port']) + ' ' * (
                                      36 - i - len(hosts_info[server_uuid]['host'])),
                          ''.join(v)
                          )
        print(hv)
    if  hosts_info[server_uuid]['slave_host_list']:
        i=i+4
        for server_uuid in hosts_info[server_uuid]['slave_host_list'] :
            if server_uuid not in co_master_uuids:
                print_topo(server_uuid=server_uuid, i=i)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument("h")
    parser.add_argument("-P", type=int, default=3306)
    args = parser.parse_args()
    host = args.h
    port = args.P
    get_hosts_info(host=host, port=port)

    title_list = [str(print_dict[k]) + ' ' * (4) for k in print_dict]
    title_str = "%s%s" % (' ' * (42 - 0),
                          ''.join(title_list)
                          )
    #
    print(title_str)
    if master_uuid:
        print_topo(server_uuid=master_uuid)
    else:
        for server_uuid in co_master_uuids:
            print_topo(server_uuid=server_uuid)
