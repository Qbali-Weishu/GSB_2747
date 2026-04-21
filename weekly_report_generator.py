#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
周报数据自动化生成脚本
通过配置文件定义数据处理流程，实现自动化数据清洗、关联和指标计算
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import time

import pandas as pd
import numpy as np
import yaml


class StepStatus(Enum):
    PENDING = "待执行"
    RUNNING = "执行中"
    SUCCESS = "成功"
    FAILED = "失败"
    SKIPPED = "跳过"


@dataclass
class StepResult:
    step_name: str
    status: StepStatus
    message: str
    start_time: datetime = None
    end_time: datetime = None
    row_count: int = None
    error_details: str = None
    
    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class WeeklyReportGenerator:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = None
        self.logger = None
        self.execution_results: List[StepResult] = []
        self.data_tables: Dict[str, pd.DataFrame] = {}
        self.metrics_results: Dict[str, Any] = {}
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def load_config(self) -> bool:
        step_result = StepResult(
            step_name="加载配置文件",
            status=StepStatus.RUNNING,
            message="正在加载配置文件...",
            start_time=datetime.now()
        )
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            if not self.config or 'weekly_report' not in self.config:
                raise ValueError("配置文件格式错误，缺少 weekly_report 节点")
            
            step_result.status = StepStatus.SUCCESS
            step_result.message = f"配置文件加载成功，版本: {self.config['weekly_report'].get('version', '未知')}"
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"配置文件加载失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            self._log_error(step_result)
            return False
    
    def setup_logging(self) -> bool:
        step_result = StepResult(
            step_name="初始化日志系统",
            status=StepStatus.RUNNING,
            message="正在初始化日志系统...",
            start_time=datetime.now()
        )
        
        try:
            log_config = self.config['weekly_report'].get('logging', {})
            log_level = log_config.get('level', 'INFO')
            log_file_template = log_config.get('file', 'logs/weekly_report.log')
            log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
            console_output = log_config.get('console_output', True)
            
            log_file = log_file_template.replace('{timestamp}', self.timestamp)
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            self.logger = logging.getLogger('WeeklyReportGenerator')
            self.logger.setLevel(getattr(logging, log_level.upper()))
            
            self.logger.handlers.clear()
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(getattr(logging, log_level.upper()))
            file_formatter = logging.Formatter(log_format)
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
            
            if console_output:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(getattr(logging, log_level.upper()))
                console_formatter = logging.Formatter(log_format)
                console_handler.setFormatter(console_formatter)
                self.logger.addHandler(console_handler)
            
            step_result.status = StepStatus.SUCCESS
            step_result.message = f"日志系统初始化成功，日志文件: {log_file}"
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"日志系统初始化失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            print(f"错误: {step_result.message}")
            return False
    
    def load_data_sources(self) -> bool:
        step_result = StepResult(
            step_name="加载数据源",
            status=StepStatus.RUNNING,
            message="正在加载数据源...",
            start_time=datetime.now()
        )
        
        try:
            data_sources = self.config['weekly_report'].get('data_sources', [])
            if not data_sources:
                raise ValueError("配置中未定义数据源")
            
            total_rows = 0
            for source in data_sources:
                source_name = source.get('name')
                source_type = source.get('type', 'csv')
                source_path = source.get('path')
                encoding = source.get('encoding', 'utf-8')
                delimiter = source.get('delimiter', ',')
                expected_columns = source.get('expected_columns', [])
                
                self._log_info(StepResult(
                    step_name=f"加载数据源: {source_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在从 {source_path} 加载数据..."
                ))
                
                if not os.path.exists(source_path):
                    raise FileNotFoundError(f"数据源文件不存在: {source_path}")
                
                if source_type.lower() == 'csv':
                    df = pd.read_csv(
                        source_path,
                        encoding=encoding,
                        delimiter=delimiter,
                        dtype=str
                    )
                else:
                    raise ValueError(f"不支持的数据源类型: {source_type}")
                
                if expected_columns:
                    missing_cols = [col for col in expected_columns if col not in df.columns]
                    if missing_cols:
                        raise ValueError(f"数据源 {source_name} 缺少预期列: {missing_cols}")
                
                self.data_tables[source_name] = df
                total_rows += len(df)
                
                self._log_info(StepResult(
                    step_name=f"加载数据源: {source_name}",
                    status=StepStatus.SUCCESS,
                    message=f"成功加载 {len(df)} 行数据",
                    row_count=len(df)
                ))
            
            step_result.status = StepStatus.SUCCESS
            step_result.message = f"所有数据源加载成功，共 {total_rows} 行数据"
            step_result.row_count = total_rows
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"数据源加载失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            self._log_error(step_result)
            return False
    
    def apply_cleansing_rules(self) -> bool:
        step_result = StepResult(
            step_name="数据清洗",
            status=StepStatus.RUNNING,
            message="正在应用数据清洗规则...",
            start_time=datetime.now()
        )
        
        try:
            cleansing_rules = self.config['weekly_report'].get('cleansing_rules', [])
            if not cleansing_rules:
                self._log_info(StepResult(
                    step_name="数据清洗",
                    status=StepStatus.SKIPPED,
                    message="未定义清洗规则，跳过数据清洗步骤"
                ))
                return True
            
            for rule_set in cleansing_rules:
                source_name = rule_set.get('data_source')
                rules = rule_set.get('rules', [])
                
                if source_name not in self.data_tables:
                    raise ValueError(f"数据源 {source_name} 不存在")
                
                df = self.data_tables[source_name].copy()
                
                self._log_info(StepResult(
                    step_name=f"数据清洗: {source_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在应用 {len(rules)} 条清洗规则..."
                ))
                
                for rule in rules:
                    field = rule.get('field')
                    action = rule.get('action')
                    params = rule.get('params', {})
                    
                    if field not in df.columns:
                        self._log_warning(StepResult(
                            step_name=f"数据清洗: {source_name}",
                            status=StepStatus.SKIPPED,
                            message=f"字段 {field} 不存在，跳过该规则"
                        ))
                        continue
                    
                    df = self._apply_single_cleansing_rule(df, field, action, params, source_name)
                
                self.data_tables[source_name] = df
                
                self._log_info(StepResult(
                    step_name=f"数据清洗: {source_name}",
                    status=StepStatus.SUCCESS,
                    message=f"数据清洗完成，共 {len(df)} 行数据",
                    row_count=len(df)
                ))
            
            step_result.status = StepStatus.SUCCESS
            step_result.message = "所有数据清洗规则应用成功"
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"数据清洗失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            self._log_error(step_result)
            return False
    
    def _apply_single_cleansing_rule(self, df: pd.DataFrame, field: str, 
                                      action: str, params: Dict, source_name: str) -> pd.DataFrame:
        if action == 'fill_null':
            fill_value = params.get('value')
            df[field] = df[field].fillna(fill_value)
            self._log_debug(StepResult(
                step_name=f"数据清洗: {source_name}",
                status=StepStatus.SUCCESS,
                message=f"字段 {field}: 空值填充为 {fill_value}"
            ))
            
        elif action == 'ensure_numeric':
            numeric_type = params.get('type', 'float')
            if numeric_type == 'int':
                df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0).astype(int)
            else:
                df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0.0)
            self._log_debug(StepResult(
                step_name=f"数据清洗: {source_name}",
                status=StepStatus.SUCCESS,
                message=f"字段 {field}: 转换为数值类型 ({numeric_type})"
            ))
            
        elif action == 'format_date':
            input_formats = params.get('input_formats', ['%Y-%m-%d'])
            output_format = params.get('output_format', '%Y-%m-%d')
            
            def parse_date(val):
                if pd.isna(val) or val == '':
                    return pd.NaT
                for fmt in input_formats:
                    try:
                        return pd.to_datetime(val, format=fmt)
                    except (ValueError, TypeError):
                        continue
                return pd.NaT
            
            df[field] = df[field].apply(parse_date)
            df[field] = df[field].dt.strftime(output_format)
            self._log_debug(StepResult(
                step_name=f"数据清洗: {source_name}",
                status=StepStatus.SUCCESS,
                message=f"字段 {field}: 日期格式化为 {output_format}"
            ))
            
        elif action == 'normalize_category':
            mapping = params.get('mapping', {})
            default = params.get('default', df[field])
            
            df[field] = df[field].apply(
                lambda x: mapping.get(str(x).strip(), 
                                     mapping.get(str(x), default if pd.notna(x) else default))
            )
            self._log_debug(StepResult(
                step_name=f"数据清洗: {source_name}",
                status=StepStatus.SUCCESS,
                message=f"字段 {field}: 类别标准化完成"
            ))
        
        return df
    
    def apply_field_mapping(self) -> bool:
        step_result = StepResult(
            step_name="字段映射",
            status=StepStatus.RUNNING,
            message="正在应用字段映射...",
            start_time=datetime.now()
        )
        
        try:
            field_mappings = self.config['weekly_report'].get('field_mapping', [])
            if not field_mappings:
                self._log_info(StepResult(
                    step_name="字段映射",
                    status=StepStatus.SKIPPED,
                    message="未定义字段映射，跳过该步骤"
                ))
                return True
            
            for mapping_set in field_mappings:
                source_name = mapping_set.get('source')
                mappings = mapping_set.get('mappings', [])
                
                if source_name not in self.data_tables:
                    raise ValueError(f"数据源 {source_name} 不存在")
                
                df = self.data_tables[source_name].copy()
                
                self._log_info(StepResult(
                    step_name=f"字段映射: {source_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在应用 {len(mappings)} 条字段映射..."
                ))
                
                for mapping in mappings:
                    from_field = mapping.get('from')
                    to_field = mapping.get('to')
                    
                    if from_field not in df.columns:
                        self._log_warning(StepResult(
                            step_name=f"字段映射: {source_name}",
                            status=StepStatus.SKIPPED,
                            message=f"源字段 {from_field} 不存在，跳过该映射"
                        ))
                        continue
                    
                    if to_field != from_field:
                        df[to_field] = df[from_field]
                        df = df.drop(columns=[from_field])
                    
                    self._log_debug(StepResult(
                        step_name=f"字段映射: {source_name}",
                        status=StepStatus.SUCCESS,
                        message=f"字段映射: {from_field} -> {to_field}"
                    ))
                
                self.data_tables[source_name] = df
                
                self._log_info(StepResult(
                    step_name=f"字段映射: {source_name}",
                    status=StepStatus.SUCCESS,
                    message=f"字段映射完成，剩余字段: {list(df.columns)}"
                ))
            
            step_result.status = StepStatus.SUCCESS
            step_result.message = "所有字段映射应用成功"
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"字段映射失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            self._log_error(step_result)
            return False
    
    def perform_table_joins(self) -> bool:
        step_result = StepResult(
            step_name="表间关联",
            status=StepStatus.RUNNING,
            message="正在执行表间关联...",
            start_time=datetime.now()
        )
        
        try:
            table_joins = self.config['weekly_report'].get('table_joins', [])
            if not table_joins:
                self._log_info(StepResult(
                    step_name="表间关联",
                    status=StepStatus.SKIPPED,
                    message="未定义表间关联，跳过该步骤"
                ))
                return True
            
            for join_config in table_joins:
                join_name = join_config.get('name')
                left_table = join_config.get('left_table')
                right_table = join_config.get('right_table')
                join_type = join_config.get('join_type', 'inner')
                join_keys = join_config.get('join_keys', [])
                output_table = join_config.get('output_table')
                
                self._log_info(StepResult(
                    step_name=f"表间关联: {join_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在关联 {left_table} 和 {right_table}..."
                ))
                
                if left_table not in self.data_tables:
                    raise ValueError(f"左表 {left_table} 不存在")
                if right_table not in self.data_tables:
                    raise ValueError(f"右表 {right_table} 不存在")
                
                left_df = self.data_tables[left_table].copy()
                right_df = self.data_tables[right_table].copy()
                
                left_keys = [k.get('left') for k in join_keys]
                right_keys = [k.get('right') for k in join_keys]
                
                if len(left_keys) != len(right_keys):
                    raise ValueError("关联键数量不匹配")
                
                for lk, rk in zip(left_keys, right_keys):
                    if lk not in left_df.columns:
                        raise ValueError(f"左表缺少关联键: {lk}")
                    if rk not in right_df.columns:
                        raise ValueError(f"右表缺少关联键: {rk}")
                
                suffixes = ('_left', '_right')
                result_df = pd.merge(
                    left_df,
                    right_df,
                    left_on=left_keys,
                    right_on=right_keys,
                    how=join_type,
                    suffixes=suffixes
                )
                
                for col in result_df.columns:
                    if col.endswith('_left'):
                        base_col = col[:-5]
                        if f"{base_col}_right" in result_df.columns:
                            result_df[base_col] = result_df[col].combine_first(result_df[f"{base_col}_right"])
                            result_df = result_df.drop(columns=[col, f"{base_col}_right"])
                
                self.data_tables[output_table] = result_df
                
                self._log_info(StepResult(
                    step_name=f"表间关联: {join_name}",
                    status=StepStatus.SUCCESS,
                    message=f"关联完成，生成 {output_table}，共 {len(result_df)} 行数据",
                    row_count=len(result_df)
                ))
            
            step_result.status = StepStatus.SUCCESS
            step_result.message = "所有表间关联执行成功"
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"表间关联失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            self._log_error(step_result)
            return False
    
    def calculate_metrics(self) -> bool:
        step_result = StepResult(
            step_name="指标计算",
            status=StepStatus.RUNNING,
            message="正在计算指标...",
            start_time=datetime.now()
        )
        
        try:
            metrics_config = self.config['weekly_report'].get('metrics_calculation', [])
            if not metrics_config:
                self._log_info(StepResult(
                    step_name="指标计算",
                    status=StepStatus.SKIPPED,
                    message="未定义指标计算，跳过该步骤"
                ))
                return True
            
            for calc_config in metrics_config:
                calc_name = calc_config.get('name')
                source_table = calc_config.get('source_table')
                group_by = calc_config.get('group_by', [])
                metrics = calc_config.get('metrics', [])
                
                self._log_info(StepResult(
                    step_name=f"指标计算: {calc_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在计算 {len(metrics)} 个指标..."
                ))
                
                if source_table not in self.data_tables:
                    raise ValueError(f"源表 {source_table} 不存在")
                
                df = self.data_tables[source_table].copy()
                
                if group_by:
                    result_df = self._calculate_grouped_metrics(df, group_by, metrics, calc_name)
                else:
                    result_df = self._calculate_overall_metrics(df, metrics, calc_name)
                
                self.metrics_results[calc_name] = result_df
                
                self._log_info(StepResult(
                    step_name=f"指标计算: {calc_name}",
                    status=StepStatus.SUCCESS,
                    message=f"指标计算完成，共 {len(result_df)} 行结果",
                    row_count=len(result_df)
                ))
            
            step_result.status = StepStatus.SUCCESS
            step_result.message = "所有指标计算完成"
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"指标计算失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            self._log_error(step_result)
            return False
    
    def _calculate_overall_metrics(self, df: pd.DataFrame, metrics: List[Dict], 
                                     calc_name: str) -> pd.DataFrame:
        results = {}
        
        for metric in metrics:
            metric_name = metric.get('name')
            metric_type = metric.get('type')
            field = metric.get('field')
            
            if field not in df.columns:
                self._log_warning(StepResult(
                    step_name=f"指标计算: {calc_name}",
                    status=StepStatus.SKIPPED,
                    message=f"字段 {field} 不存在，跳过指标 {metric_name}"
                ))
                continue
            
            if metric_type == 'count':
                results[metric_name] = df[field].count()
            elif metric_type == 'count_distinct':
                results[metric_name] = df[field].nunique()
            elif metric_type == 'sum':
                results[metric_name] = pd.to_numeric(df[field], errors='coerce').sum()
            elif metric_type == 'avg':
                results[metric_name] = pd.to_numeric(df[field], errors='coerce').mean()
            elif metric_type == 'max':
                results[metric_name] = pd.to_numeric(df[field], errors='coerce').max()
            elif metric_type == 'min':
                results[metric_name] = pd.to_numeric(df[field], errors='coerce').min()
            
            self._log_debug(StepResult(
                step_name=f"指标计算: {calc_name}",
                status=StepStatus.SUCCESS,
                message=f"指标 {metric_name} = {results.get(metric_name)}"
            ))
        
        return pd.DataFrame([results])
    
    def _calculate_grouped_metrics(self, df: pd.DataFrame, group_by: List[str], 
                                    metrics: List[Dict], calc_name: str) -> pd.DataFrame:
        for gb in group_by:
            if gb not in df.columns:
                raise ValueError(f"分组字段 {gb} 不存在")
        
        results_list = []
        
        grouped = df.groupby(group_by, dropna=False)
        
        for group_name, group_df in grouped:
            group_result = {}
            
            if isinstance(group_name, tuple):
                for i, gb in enumerate(group_by):
                    group_result[gb] = group_name[i]
            else:
                group_result[group_by[0]] = group_name
            
            for metric in metrics:
                metric_name = metric.get('name')
                metric_type = metric.get('type')
                field = metric.get('field')
                
                if field not in group_df.columns:
                    continue
                
                if metric_type == 'count':
                    group_result[metric_name] = group_df[field].count()
                elif metric_type == 'count_distinct':
                    group_result[metric_name] = group_df[field].nunique()
                elif metric_type == 'sum':
                    group_result[metric_name] = pd.to_numeric(group_df[field], errors='coerce').sum()
                elif metric_type == 'avg':
                    group_result[metric_name] = pd.to_numeric(group_df[field], errors='coerce').mean()
                elif metric_type == 'max':
                    group_result[metric_name] = pd.to_numeric(group_df[field], errors='coerce').max()
                elif metric_type == 'min':
                    group_result[metric_name] = pd.to_numeric(group_df[field], errors='coerce').min()
            
            results_list.append(group_result)
        
        return pd.DataFrame(results_list)
    
    def generate_outputs(self) -> bool:
        step_result = StepResult(
            step_name="生成输出",
            status=StepStatus.RUNNING,
            message="正在生成输出文件...",
            start_time=datetime.now()
        )
        
        try:
            output_configs = self.config['weekly_report'].get('output', [])
            if not output_configs:
                self._log_info(StepResult(
                    step_name="生成输出",
                    status=StepStatus.SKIPPED,
                    message="未定义输出配置，跳过该步骤"
                ))
                return True
            
            for output_config in output_configs:
                output_name = output_config.get('name')
                output_type = output_config.get('type', 'csv')
                output_path_template = output_config.get('path')
                encoding = output_config.get('encoding', 'utf-8')
                delimiter = output_config.get('delimiter', ',')
                include_timestamp = output_config.get('include_timestamp', False)
                
                self._log_info(StepResult(
                    step_name=f"生成输出: {output_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在生成输出文件..."
                ))
                
                if output_name == "周报核心指标汇总表":
                    if "基础指标" in self.metrics_results:
                        df = self.metrics_results["基础指标"]
                    else:
                        raise ValueError("基础指标计算结果不存在")
                elif output_name == "按渠道分析表":
                    if "按渠道分组" in self.metrics_results:
                        df = self.metrics_results["按渠道分组"]
                    else:
                        raise ValueError("渠道分组指标计算结果不存在")
                elif output_name == "按客户等级分析表":
                    if "按客户等级分组" in self.metrics_results:
                        df = self.metrics_results["按客户等级分组"]
                    else:
                        raise ValueError("客户等级分组指标计算结果不存在")
                elif output_name == "按区域分析表":
                    if "按区域分组" in self.metrics_results:
                        df = self.metrics_results["按区域分组"]
                    else:
                        raise ValueError("区域分组指标计算结果不存在")
                elif output_name == "完整关联明细数据":
                    if "完整关联表" in self.data_tables:
                        df = self.data_tables["完整关联表"]
                    else:
                        raise ValueError("完整关联表不存在")
                else:
                    self._log_warning(StepResult(
                        step_name=f"生成输出: {output_name}",
                        status=StepStatus.SKIPPED,
                        message=f"未知的输出类型: {output_name}"
                    ))
                    continue
                
                output_path = output_path_template
                if include_timestamp:
                    base, ext = os.path.splitext(output_path)
                    output_path = f"{base}_{self.timestamp}{ext}"
                
                output_dir = os.path.dirname(output_path)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir, exist_ok=True)
                
                if output_type.lower() == 'csv':
                    df.to_csv(
                        output_path,
                        index=False,
                        encoding=encoding,
                        sep=delimiter
                    )
                else:
                    raise ValueError(f"不支持的输出类型: {output_type}")
                
                self._log_info(StepResult(
                    step_name=f"生成输出: {output_name}",
                    status=StepStatus.SUCCESS,
                    message=f"输出文件已生成: {output_path}，共 {len(df)} 行",
                    row_count=len(df)
                ))
            
            step_result.status = StepStatus.SUCCESS
            step_result.message = "所有输出文件生成成功"
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"生成输出失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            self._log_error(step_result)
            return False
    
    def run(self) -> bool:
        self._log_info(StepResult(
            step_name="周报生成流程",
            status=StepStatus.RUNNING,
            message=f"开始执行周报数据生成流程，时间戳: {self.timestamp}"
        ))
        
        steps = [
            ("加载配置文件", self.load_config),
            ("初始化日志系统", self.setup_logging),
            ("加载数据源", self.load_data_sources),
            ("数据清洗", self.apply_cleansing_rules),
            ("字段映射", self.apply_field_mapping),
            ("表间关联", self.perform_table_joins),
            ("指标计算", self.calculate_metrics),
            ("生成输出", self.generate_outputs),
        ]
        
        for step_name, step_func in steps:
            self._log_info(StepResult(
                step_name="流程控制",
                status=StepStatus.RUNNING,
                message=f"开始执行步骤: {step_name}"
            ))
            
            success = step_func()
            
            if not success:
                error_handling = self.config['weekly_report'].get('error_handling', {}) if self.config else {}
                stop_on_error = error_handling.get('stop_on_error', True)
                
                if stop_on_error:
                    self._log_error(StepResult(
                        step_name="流程控制",
                        status=StepStatus.FAILED,
                        message=f"步骤 {step_name} 失败，流程终止"
                    ))
                    return False
                else:
                    self._log_warning(StepResult(
                        step_name="流程控制",
                        status=StepStatus.SKIPPED,
                        message=f"步骤 {step_name} 失败，继续执行后续步骤"
                    ))
        
        self._log_info(StepResult(
            step_name="周报生成流程",
            status=StepStatus.SUCCESS,
            message="周报数据生成流程执行完成！"
        ))
        
        return True
    
    def _log_info(self, step_result: StepResult):
        if self.logger:
            extra = {'step': step_result.step_name}
            self.logger.info(step_result.message, extra=extra)
        else:
            print(f"[INFO] [{step_result.step_name}] {step_result.message}")
        self.execution_results.append(step_result)
    
    def _log_warning(self, step_result: StepResult):
        if self.logger:
            extra = {'step': step_result.step_name}
            self.logger.warning(step_result.message, extra=extra)
        else:
            print(f"[WARNING] [{step_result.step_name}] {step_result.message}")
        self.execution_results.append(step_result)
    
    def _log_error(self, step_result: StepResult):
        if self.logger:
            extra = {'step': step_result.step_name}
            self.logger.error(step_result.message, extra=extra)
            if step_result.error_details:
                self.logger.error(f"详细错误:\n{step_result.error_details}", extra=extra)
        else:
            print(f"[ERROR] [{step_result.step_name}] {step_result.message}")
            if step_result.error_details:
                print(f"详细错误:\n{step_result.error_details}")
        self.execution_results.append(step_result)
    
    def _log_debug(self, step_result: StepResult):
        if self.logger:
            extra = {'step': step_result.step_name}
            self.logger.debug(step_result.message, extra=extra)


class WeeklyReportValidator:
    def __init__(self, generator: WeeklyReportGenerator):
        self.generator = generator
        self.validation_results = []
    
    def validate_with_expected(self, expected_results_path: str) -> Dict[str, Any]:
        validation_result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'overall_status': 'PENDING',
            'checks': []
        }
        
        try:
            with open(expected_results_path, 'r', encoding='utf-8') as f:
                expected = yaml.safe_load(f)
            
            for check_name, check_config in expected.get('validations', {}).items():
                check_result = self._perform_check(check_name, check_config)
                validation_result['checks'].append(check_result)
            
            all_passed = all(c.get('status') == 'PASS' for c in validation_result['checks'])
            validation_result['overall_status'] = 'PASS' if all_passed else 'FAIL'
            
            return validation_result
            
        except Exception as e:
            validation_result['overall_status'] = 'ERROR'
            validation_result['error'] = str(e)
            return validation_result
    
    def _perform_check(self, check_name: str, check_config: Dict) -> Dict:
        result = {
            'check_name': check_name,
            'status': 'PENDING',
            'message': '',
            'expected': None,
            'actual': None
        }
        
        try:
            check_type = check_config.get('type')
            source = check_config.get('source')
            field = check_config.get('field')
            expected_value = check_config.get('expected')
            
            if source in self.generator.metrics_results:
                df = self.generator.metrics_results[source]
            elif source in self.generator.data_tables:
                df = self.generator.data_tables[source]
            else:
                result['status'] = 'SKIP'
                result['message'] = f"数据源 {source} 不存在"
                return result
            
            if check_type == 'row_count':
                actual = len(df)
                result['expected'] = expected_value
                result['actual'] = actual
                
                if actual == expected_value:
                    result['status'] = 'PASS'
                    result['message'] = f"行数检查通过: {actual}"
                else:
                    result['status'] = 'FAIL'
                    result['message'] = f"行数不匹配: 预期 {expected_value}, 实际 {actual}"
            
            elif check_type == 'sum':
                if field not in df.columns:
                    result['status'] = 'SKIP'
                    result['message'] = f"字段 {field} 不存在"
                    return result
                
                actual = pd.to_numeric(df[field], errors='coerce').sum()
                result['expected'] = expected_value
                result['actual'] = round(actual, 2)
                
                if abs(actual - expected_value) < 0.01:
                    result['status'] = 'PASS'
                    result['message'] = f"求和检查通过: {actual}"
                else:
                    result['status'] = 'FAIL'
                    result['message'] = f"求和不匹配: 预期 {expected_value}, 实际 {actual}"
            
            elif check_type == 'value':
                if field not in df.columns:
                    result['status'] = 'SKIP'
                    result['message'] = f"字段 {field} 不存在"
                    return result
                
                actual = df.iloc[0][field] if len(df) > 0 else None
                result['expected'] = expected_value
                result['actual'] = actual
                
                if actual == expected_value:
                    result['status'] = 'PASS'
                    result['message'] = f"值检查通过: {actual}"
                else:
                    result['status'] = 'FAIL'
                    result['message'] = f"值不匹配: 预期 {expected_value}, 实际 {actual}"
            
            elif check_type == 'contains':
                if field not in df.columns:
                    result['status'] = 'SKIP'
                    result['message'] = f"字段 {field} 不存在"
                    return result
                
                contains = expected_value in df[field].values
                result['expected'] = f"包含 {expected_value}"
                result['actual'] = "包含" if contains else "不包含"
                
                if contains:
                    result['status'] = 'PASS'
                    result['message'] = f"包含检查通过: 找到 {expected_value}"
                else:
                    result['status'] = 'FAIL'
                    result['message'] = f"包含检查失败: 未找到 {expected_value}"
            
            return result
            
        except Exception as e:
            result['status'] = 'ERROR'
            result['message'] = str(e)
            return result


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='周报数据自动化生成工具')
    parser.add_argument('-c', '--config', default='config.yaml', help='配置文件路径')
    parser.add_argument('-m', '--mode', choices=['run', 'validate', 'both'], default='run',
                       help='运行模式: run(仅运行), validate(仅验证), both(运行并验证)')
    parser.add_argument('-e', '--expected', default='expected_results.yaml', 
                       help='预期结果验证文件路径')
    
    args = parser.parse_args()
    
    generator = WeeklyReportGenerator(config_path=args.config)
    
    if args.mode in ['run', 'both']:
        success = generator.run()
        
        if not success:
            print("\n流程执行失败，请检查日志获取详细信息。")
            sys.exit(1)
    
    if args.mode in ['validate', 'both']:
        print("\n开始验证结果...")
        validator = WeeklyReportValidator(generator)
        validation_result = validator.validate_with_expected(args.expected)
        
        print(f"\n验证结果: {validation_result['overall_status']}")
        for check in validation_result.get('checks', []):
            status_icon = '✓' if check['status'] == 'PASS' else '✗' if check['status'] == 'FAIL' else '?'
            print(f"  {status_icon} {check['check_name']}: {check['message']}")
        
        if validation_result['overall_status'] != 'PASS':
            sys.exit(1)
    
    print("\n所有操作完成！")


if __name__ == '__main__':
    main()
