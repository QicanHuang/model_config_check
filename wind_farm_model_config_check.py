# -*- coding: utf-8 -*-
"""
Time: 2020/12/26 18:13
Author: 29361
Version: A0
Description: 用于对风电场配置进行基本的准确性检测，配置信息以日志形式打印到控制台
"""

import logging
from dateutil import parser
import algo_data as ad


# logging.basicConfig(level=logging.INFO,
#                     filename='new_600_.log',
#                     filemode='a',
#                     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 配置气象限制，只有配置的气象是下面的气象才可以用于预测，否则不能预测
PRE_NWP_LIST = ['CMA', 'GFS', 'ECGFS', 'EC', 'ECWRF', 'COPT', 'CMA2P5', 'ECENS',
                'MIX', 'PVS', 'METE', 'METEpv', 'MLC', 'MF', 'CONWX', 'ML',
                'conwx', 'MeteoForce', 'sea', 'ECANEN', 'XZ', 'SUP']


def _read_sql(db, table_name, columns, conditions, ind_id):
    """
    数据读取，基于algo_data接口读取数据
    :param db:
    :param table_name:
    :param columns:
    :param conditions:
    :param ind_id: 特定信息标志
    :return:
    """
    try:
        db_ = ad.Api(db=db)
        data = db_.select_joint(table_name=table_name,
                                columns=columns,
                                conditions=conditions)
    except Exception as err:
        raise (Exception('Get %s data from %s failed!' % (str(ind_id), table_name), err))
    else:
        return data


def _eval(str_param):
    """
    字典字符串解析
    :param str_param: 日期字符串
    :return:
    """
    if "true" in str_param:
        str_param = str_param.replace('true', '"true"')
    if "null" in str_param:
        str_param = str_param.replace('null', '"null"')
    if "false" in str_param:
        str_param = str_param.replace('false', '"false"')
    try:
        return True, eval(str_param)
    except Exception as err:
        err_info = "'%s', 请检查字符串格式。异常信息: '%s'" % (str_param, err)
        return False, err_info


def _parser_datetime(str_time):
    """
    日期形式字符串解析
    :param str_time: 日期形式字符串
    :return:
    """
    try:
        return True, parser.parse(str_time)
    except Exception as err:
        err_info = "'%s' 日期格式异常,异常信息 '%s'" % (str_time, err)
        return False, err_info


class WindFarmModelConfig(object):
    """
    风电场配置信息获取
    """
    def __init__(self, wfid):
        self.wfid = wfid
        self._check_wfid()
        self.wf_info = self._get_wind_farm_info()
        self.cid = self.wf_info['cid']
        self.cid_bool = self._check_cid()

    def _check_wfid(self):
        """检测输入的风电场id,如果是 cid 的话则转为 wfid"""
        wfid_ = str(self.wfid).split('_')
        if len(wfid_) == 2:
            self.wfid = wfid_[1]

    def _check_cid(self):
        """检测 self.cid 是否正常"""
        return False if ((self.cid is None) or (len(self.cid) == 0)) else True

    def _get_wind_farm_info(self):
        """获取风电场基本信息"""
        try:
            wf_info = ad.farm_info(wfid=self.wfid, dtype='dict')
        except Exception as err:
            raise (Exception('查询不到相应风电场，请检查输入参数'), err)
        else:
            return wf_info

    def get_model_config(self):
        """获取风电场的模型配置，返回字典类型结果"""
        config = _read_sql(db='atp',
                           table_name='wind_farm_available',
                           columns='*',
                           conditions="cid = '%s'" % self.cid,
                           ind_id=self.cid)
        model_config = {} if config.empty else config.to_dict(orient='records')[0]
        return model_config

    def get_method_list(self):
        """获取该风电场对应的方法列表"""
        config = _read_sql(db='atp',
                           table_name='method_config',
                           columns='method_id',
                           conditions="cid = '%s'" % self.cid,
                           ind_id=self.cid)
        method_list = list(config['method_id'])
        return method_list

    @staticmethod
    def get_method_config(method_id):
        """获取给定方法的配置，返回字典类型结果"""
        config = _read_sql(db='atp',
                           table_name='method_config',
                           columns='*',
                           conditions="method_id = '%s'" % method_id,
                           ind_id=method_id)
        method_config = {} if config.empty else config.to_dict(orient='records')[0]
        return method_config

    def get_power_fix_config(self):
        """获取该风电场的后处理函数配置，返回字典类型结果"""
        config = _read_sql(db='atp',
                           table_name='power_fix_config',
                           columns='*',
                           conditions="cid = '%s'" % self.cid,
                           ind_id=self.cid)
        power_fix_config = config.set_index('id', drop=True).to_dict('index')
        return power_fix_config

    def get_nwp_config(self):
        """获取风电场的气象配置(不包括第三方气象数据)，返回列表类型结果"""
        config = _read_sql(db='indicator',
                           table_name='nwp_monitor',
                           columns='source',
                           conditions="wfid = %s" % self.wfid,
                           ind_id=self.wfid)
        return list(set(list(config['source'])))

    def get_third_list(self):
        """获取该风电场对应的第三方气象，返回字典类型结果"""
        config = _read_sql(db='WindFarmdb',
                           table_name='thirdparty_info',
                           columns='meteologica, meteoforce, conwx, xinzhi',
                           conditions="wfid = '%s'" % self.wfid,
                           ind_id=self.cid)
        if config.empty:
            return []
        else:
            third_list_dict = config.to_dict(orient='records')[0]
            third_list = [key for key, value in third_list_dict.items() if value == 1]
            return third_list


class WindFarmModelConfigCheck(WindFarmModelConfig):
    """
    风电场配置信息检测
    """
    def __init__(self, wfid):
        super(WindFarmModelConfigCheck, self).__init__(wfid)

    def _t_str(self, base_info, other_info):
        return '%s: %s%s' % (self.wfid, base_info, other_info)

    def _f_str(self, base_info, other_info):
        return 'error: %s: %s%s' % (self.wfid, base_info, other_info)

    def _check_region(self, region, base_info):
        """
        检测 区域编号 设置是否正常
        :param region:
        :param base_info:
        :return:
        """
        region_bool, region = _eval(str_param=region)
        if not region_bool:
            logger.info(self._f_str(base_info, region))
        else:
            if isinstance(region, str) or isinstance(list(region.values())[0], str):
                logger.info(self._f_str(base_info, '区域编号 容量设置异常,应设置为数值型'))
            else:
                if self.wf_info['powercap'] != sum(region.values()):
                    logger.info(self._f_str(base_info,
                                            '区域编号 容量设置异常,与风电场基本信息中容量不一致'))
                else:
                    logger.info(self._t_str(base_info, '区域编号 容量设置正常'))

    def _check_manual_model(self, manual_model, base_info):
        """
        检测 指定模型 设置是否正常
        :param manual_model:
        :param base_info:
        :return:
        """
        if not ((manual_model is None) or
                (manual_model.isspace()) or
                (len(manual_model) == 0)):
            if manual_model not in self.get_method_list():
                logger.info(self._f_str(base_info, '指定模型 指定模型异常'))
            else:
                logger.info(self._t_str(base_info, '指定模型 指定模型正常'))
        else:
            logger.info(self._t_str(base_info, "指定模型 指定模型为空"))

    def chek_model_config(self):
        """
        判断 电场列表-电场修改-区域编号 的容量设置是否正常
        """
        base_info = '电场列表-电场修改-'
        model_config = self.get_model_config()

        if model_config:
            # 解析和检测‘区域编号’对应 字段
            self._check_region(model_config['region'], base_info)
            # 检查'指定模型'是否存在
            self._check_manual_model(model_config['manual_model'], base_info)
        else:
            logger.info(self._f_str(base_info, "查询不到模型配置"))

    def check_method_config(self):
        """
        判断 '模型列表-方法名称-模型配置' 设置是否正常
        """
        nwp_list = self.get_nwp_config() + self.get_third_list()
        nwp_list_lower = [str.lower(kk) for kk in nwp_list]
        # 补充特殊气象（气象命名不一致）
        if 'ec' in nwp_list_lower:
            nwp_list_lower.append('ecgfs')
        if 'opt' in nwp_list_lower:
            nwp_list_lower.append('copt')

        # 可预测的气象源配置
        pre_nwp_list_lower = [str.lower(kk) for kk in PRE_NWP_LIST]

        for method in self.get_method_list():
            base_info = '模型列表-%s-模型配置' % method
            # 格式解析检测
            method_config = self.get_method_config(method)
            region_bool, region = _eval(str_param=method_config['region'])
            if not region_bool:
                logger.info(self._f_str(base_info, region))
            else:
                # 检查气象源设置是否正确
                err_nwp = []
                for region_ in region:
                    region_nwp_config = region_['nwp_config']
                    for key, value in region_nwp_config.items():
                        # 只有当配置气象同时在 nwp_list_lower 和 pre_nwp_list_str 时才是正确配置
                        if (str.lower(key) not in nwp_list_lower) or \
                                (str.lower(key) not in pre_nwp_list_lower):
                            err_nwp.append('%s: %s' % (region_['rid'], key))
                if err_nwp:
                    logger.info(self._f_str(base_info, " %s 气象不存在" % ','.join(err_nwp)))
                else:
                    logger.info(self._t_str(base_info, " 设置正常"))

    def __key_check(self, real_key, config_key, base_info):
        """
        检查 后处理函数配置中的主键是否正常
        real_key：正常主键
        config_key: 实际配置主键
        base_info: 日志基础信息
        """
        error_bool = False
        error_key = [kk for kk in config_key if kk not in real_key]
        if error_key:
            logger.info(self._f_str(base_info, " '%s' 字段错误" % ','.join(error_key)))
            error_bool = True
        return error_bool

    def __power_off_check(self, real_key, config_key, config, base_info):
        """
        后处理函数 power_off 的 config 字典的值检测
        :param real_key:
        :param config_key:
        :param config:
        :param base_info:
        :return:
        """
        error_bool = False
        for dt_type in real_key[:-1]:
            if dt_type in config_key:
                dt_bool, dt_info = _parser_datetime(config[dt_type])
                if not dt_bool:
                    logger.info(self._f_str(base_info, " '%s' 字段错误" % dt_info))
                    error_bool = True
        if real_key[-1] in config_key:
            temp = config[real_key[-1]]
            if (not isinstance(temp, int)) and (not isinstance(temp, float)):
                logger.info(self._f_str(base_info,
                                        " '%s' 字段错误，需要设置为数值型" % str(temp)))
                error_bool = True
        return error_bool

    def __fix_power_check(self, config, base_info):
        """
        后处理函数 fix_power 的 config 字典的值检测
        :param config:
        :param base_info:
        :return:
        """
        error_bool = False
        set_value = config['set_value']
        if 'predict_power' in set_value:
            if set_value.split('*')[0] == 'predict_power':
                logger.info(self._f_str(base_info,
                                        " 'set_value'应设置为'1.0*predict_power'形式"))
                error_bool = True
        return error_bool

    def _check_power_fix_param(self, power_fix, base_info):
        """
        具体某个后处理函数检测
        power_fix：后处理函数配置字典
        base_info：
        """
        func_name = power_fix['func_name']
        config_bool, config = _eval(str_param=power_fix['config'])
        if not config_bool:
            logger.info(self._f_str(base_info, config))
        else:
            # 后处理函数配置字典的主键列表
            config_key = list(config.keys())
            # 后处理函数整体是否存在异常的标志
            error_bool = False

            if func_name == 'power_off':
                real_key = ['start_time', 'end_time', 'discount']
                err_exist_1 = self.__key_check(real_key, config_key, base_info)
                err_exist_2 = self.__power_off_check(real_key, config_key,
                                                     config, base_info)
                if err_exist_1 or err_exist_2:
                    error_bool = True

            if func_name == 'fix_power':
                real_key = ['options', 'set_value']
                err_exist_1 = self.__key_check(real_key, config_key, base_info)
                err_exist_2 = self.__fix_power_check(config, base_info)
                if err_exist_1 or err_exist_2:
                    error_bool = True

            if func_name == 'line_fix':
                real_key = ['h']
                error_bool = self.__key_check(real_key, config_key, base_info)

            if not error_bool:
                logger.info(self._t_str(base_info, " 设置正常"))

    def check_power_fix_config(self):
        """
        后处理函数检测
        """
        power_fix_func = ['power_off', 'line_fix', 'fix_power', 'set_limit']
        power_fix_config = self.get_power_fix_config()
        if power_fix_config:
            for power_fix_id, power_fix in power_fix_config.items():
                # 检测后处理函数名称
                func_name = power_fix['func_name']
                base_info = "'后处理函数-%s-%s'" % (str(power_fix_id), func_name)
                if func_name not in power_fix_func:
                    logger.info(self._f_str(base_info, " 函数名异常"))
                else:
                    # 检测后处理函数配置
                    self._check_power_fix_param(power_fix, base_info)
        else:
            logger.info(self._t_str('', " 无后处理函数"))

    def check_all(self):
        """检测整个风电场的配置信息"""
        if not self.cid_bool:
            logger.info(self._f_str(self.wfid, " 电场cid异常 cid='%s'" % self.cid))
        else:
            self.chek_model_config()
            logger.info('\n')
            self.check_method_config()
            logger.info('\n')
            self.check_power_fix_config()
            logger.info('\n')


def wind_farm_check(wfid):
    """
    风电场配置检测入口函数
    :param wfid: 风电场wf_id,如 wfid='320908' 或 wfid=['320908','836610']
    """
    if isinstance(wfid, list):
        for i, wfid_ in enumerate(wfid):
            print(i, ":", wfid_)
            wf_config_check_obj = WindFarmModelConfigCheck(wfid=wfid_)
            wf_config_check_obj.check_all()
    else:
        wf_config_check_obj = WindFarmModelConfigCheck(wfid=str(wfid))
        wf_config_check_obj.check_all()


# def log_static(log_path_list):
#     import pandas as pd
#     db_ = ad.Api(db='WindFarmdb')
#     wf_info = db_.select_joint(table_name='wind_farm_info',
#                                columns='wfid, wfname, administrator, f_type, inspection_state')
#     wf_info_dict = wf_info.set_index(keys=['wfid'], drop=True).to_dict(orient='index')
#
#     static_dict = []
#     for log_path in log_path_list:
#         print(log_path)
#         with open(log_path, 'r+', encoding='gbk') as log_file:
#             for line in log_file.readlines():
#                 log_info = line.split('__main__ - INFO - ')
#                 if len(log_info) > 1:
#                     error_info = log_info[1].replace('\n', '')
#                     if 'error:' in error_info:
#                         log_info_split = error_info.split(':')
#                         wf_id = log_info_split[1].strip()
#                         static_dict.append({**{'wf_id': wf_id, 'error_info': error_info},
#                                             **wf_info_dict[wf_id]})
#     static_df = pd.DataFrame.from_dict(static_dict)
#     static_df.to_csv('log.csv', index=True, encoding='gbk')


if __name__ == '__main__':
    db_api = ad.Api(db='WindFarmdb')
    data_select = db_api.select_joint(table_name='wind_farm_info',
                                      columns='wfid, project_background',
                                      conditions="inspection_state != '停用' and f_type = 'W'")
    wf_id_list = list(data_select[data_select['project_background'] !=
                                  '[{"central":{"甘肃电科院":"气象"}}]']['wfid'])
    wf_id_list.sort()
    print(len(wf_id_list))
    # wind_farm_check(wfid=wf_id_list[600:])
    wind_farm_check(wfid=['320908'])

    # log_static(['new_0_200.log', 'new_200_400.log', 'new_400_600.log', 'new_600_.log'])
