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
import pickle
import gzip
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import argparse

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
    
    def to_dict(self) -> Dict:
        return {
            'step_name': self.step_name,
            'status': self.status.value,
            'message': self.message,
            'row_count': self.row_count,
            'duration': self.duration
        }


class StepFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'step'):
            record.step = '未指定'
        return True


class ResultsSerializer:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.metadata_file = os.path.join(output_dir, 'run_metadata.yaml')
        self.data_file = os.path.join(output_dir, 'run_data.pkl.gz')
    
    def save(self, generator: 'WeeklyReportGenerator') -> bool:
        try:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir, exist_ok=True)
            
            metadata = {
                'timestamp': generator.timestamp,
                'week_info': generator.week_info,
                'week_start_date': generator.week_start_date.strftime('%Y-%m-%d') if generator.week_start_date else None,
                'week_end_date': generator.week_end_date.strftime('%Y-%m-%d') if generator.week_end_date else None,
                'execution_results': [r.to_dict() for r in generator.execution_results],
                'metrics_id_map': generator.metrics_id_map,
                'data_tables_info': {name: {'columns': list(df.columns), 'row_count': len(df)} 
                                    for name, df in generator.data_tables.items()},
                'metrics_results_info': {name: {'columns': list(df.columns), 'row_count': len(df)} 
                                        for name, df in generator.metrics_results.items()},
                'saved_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                yaml.dump(metadata, f, allow_unicode=True, default_flow_style=False)
            
            data_to_save = {
                'data_tables': generator.data_tables,
                'metrics_results': generator.metrics_results,
                'metrics_id_map': generator.metrics_id_map,
                'week_info': generator.week_info,
                'timestamp': generator.timestamp
            }
            
            with gzip.open(self.data_file, 'wb') as f:
                pickle.dump(data_to_save, f)
            
            return True
            
        except Exception as e:
            print(f"保存运行结果失败: {str(e)}")
            return False
    
    def load(self, generator: 'WeeklyReportGenerator') -> bool:
        try:
            if not os.path.exists(self.metadata_file):
                print(f"元数据文件不存在: {self.metadata_file}")
                return False
            
            if not os.path.exists(self.data_file):
                print(f"数据文件不存在: {self.data_file}")
                return False
            
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                metadata = yaml.safe_load(f)
            
            with gzip.open(self.data_file, 'rb') as f:
                loaded_data = pickle.load(f)
            
            generator.data_tables = loaded_data.get('data_tables', {})
            generator.metrics_results = loaded_data.get('metrics_results', {})
            generator.metrics_id_map = loaded_data.get('metrics_id_map', {})
            generator.week_info = loaded_data.get('week_info', {})
            generator.timestamp = loaded_data.get('timestamp', generator.timestamp)
            
            if metadata.get('week_start_date'):
                generator.week_start_date = datetime.strptime(metadata['week_start_date'], '%Y-%m-%d')
            if metadata.get('week_end_date'):
                generator.week_end_date = datetime.strptime(metadata['week_end_date'], '%Y-%m-%d')
            
            return True
            
        except Exception as e:
            print(f"加载运行结果失败: {str(e)}")
            traceback.print_exc()
            return False


class WeeklyReportGenerator:
    def __init__(self, config_path: str = "config.yaml", 
                 report_date: Optional[str] = None,
                 week_start_day: Optional[int] = None,
                 custom_start_date: Optional[str] = None,
                 custom_end_date: Optional[str] = None):
        
        self.config_path_original = config_path
        self.config_path = os.path.abspath(config_path)
        self.config_dir = os.path.dirname(self.config_path)
        
        self.config = None
        self.logger = None
        self.execution_results: List[StepResult] = []
        self.data_tables: Dict[str, pd.DataFrame] = {}
        self.metrics_results: Dict[str, pd.DataFrame] = {}
        self.metrics_id_map: Dict[str, str] = {}
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        self.report_date_override = report_date
        self.week_start_day_override = week_start_day
        self.custom_start_date_override = custom_start_date
        self.custom_end_date_override = custom_end_date
        
        self.week_start_date: Optional[datetime] = None
        self.week_end_date: Optional[datetime] = None
        self.week_info: Dict[str, Any] = {}
        
        self._is_loaded = False
        self._has_run = False
    
    def _resolve_path(self, path: str) -> str:
        if os.path.isabs(path):
            return path
        return os.path.abspath(os.path.join(self.config_dir, path))
    
    def _get_results_dir(self) -> str:
        return self._resolve_path('output/last_run')
    
    def load_config(self) -> bool:
        step_result = StepResult(
            step_name="加载配置文件",
            status=StepStatus.RUNNING,
            message=f"正在加载配置文件: {self.config_path}...",
            start_time=datetime.now()
        )
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            if not self.config or 'weekly_report' not in self.config:
                raise ValueError("配置文件格式错误，缺少 weekly_report 节点")
            
            wr_config = self.config['weekly_report']
            if 'date_range' in wr_config:
                if 'auto_detect_week' in wr_config['date_range']:
                    self._log_warning(StepResult(
                        step_name="配置检查",
                        status=StepStatus.SKIPPED,
                        message="警告: auto_detect_week 配置已废弃，将被忽略"
                    ))
            
            self._is_loaded = True
            
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
            log_format = log_config.get('format', '%(asctime)s - %(levelname)s - [%(step)s] - %(message)s')
            console_output = log_config.get('console_output', True)
            
            log_file = self._resolve_path(log_file_template.replace('{timestamp}', self.timestamp))
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            self.logger = logging.getLogger('WeeklyReportGenerator')
            self.logger.setLevel(getattr(logging, log_level.upper()))
            
            self.logger.handlers.clear()
            self.logger.filters.clear()
            
            step_filter = StepFilter()
            self.logger.addFilter(step_filter)
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(getattr(logging, log_level.upper()))
            file_handler.addFilter(step_filter)
            file_formatter = logging.Formatter(log_format)
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
            
            if console_output:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(getattr(logging, log_level.upper()))
                console_handler.addFilter(step_filter)
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
    
    def calculate_week_range(self) -> bool:
        step_result = StepResult(
            step_name="计算周范围",
            status=StepStatus.RUNNING,
            message="正在计算数据周范围...",
            start_time=datetime.now()
        )
        
        try:
            wr_config = self.config['weekly_report'].get('date_range', {})
            enabled = wr_config.get('enabled', True)
            
            if not enabled:
                step_result.status = StepStatus.SKIPPED
                step_result.message = "日期范围过滤已禁用"
                step_result.end_time = datetime.now()
                self._log_info(step_result)
                return True
            
            if self.custom_start_date_override and self.custom_end_date_override:
                self.week_start_date = datetime.strptime(self.custom_start_date_override, '%Y-%m-%d')
                self.week_end_date = datetime.strptime(self.custom_end_date_override, '%Y-%m-%d')
                step_result.message = f"使用自定义日期范围: {self.week_start_date.date()} 至 {self.week_end_date.date()}"
            else:
                report_date_str = self.report_date_override or self.config['weekly_report'].get('report_date')
                if not report_date_str:
                    report_date_str = datetime.now().strftime('%Y-%m-%d')
                
                week_start_day = self.week_start_day_override or self.config['weekly_report'].get('week_start_day', 1)
                
                try:
                    report_date = datetime.strptime(report_date_str, '%Y-%m-%d')
                except ValueError:
                    raise ValueError(f"报告日期格式错误: {report_date_str}，应为 YYYY-MM-DD")
                
                days_since_start = (report_date.weekday() - week_start_day + 7) % 7
                self.week_start_date = report_date - timedelta(days=days_since_start)
                self.week_end_date = self.week_start_date + timedelta(days=6)
                
                step_result.message = f"报告日期: {report_date.date()}，周范围: {self.week_start_date.date()} 至 {self.week_end_date.date()}"
            
            self.week_info = {
                'report_date': self.week_start_date.strftime('%Y-%m-%d') if self.week_start_date else None,
                'week_start': self.week_start_date.strftime('%Y-%m-%d') if self.week_start_date else None,
                'week_end': self.week_end_date.strftime('%Y-%m-%d') if self.week_end_date else None,
                'week_number': self.week_start_date.isocalendar()[1] if self.week_start_date else None,
                'year': self.week_start_date.year if self.week_start_date else None
            }
            
            step_result.status = StepStatus.SUCCESS
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            self._log_info(StepResult(
                step_name="周范围信息",
                status=StepStatus.SUCCESS,
                message=f"周信息: {self.week_info}"
            ))
            
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"计算周范围失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            
            self._log_error(step_result)
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
            
            error_handling = self.config['weekly_report'].get('error_handling', {})
            skip_invalid_rows = error_handling.get('skip_invalid_rows', False)
            
            total_rows = 0
            for source in data_sources:
                source_name = source.get('name')
                source_id = source.get('id')
                source_type = source.get('type', 'csv')
                source_path_original = source.get('path')
                source_path = self._resolve_path(source_path_original)
                encoding = source.get('encoding', 'utf-8')
                delimiter = source.get('delimiter', ',')
                expected_columns = source.get('expected_columns', [])
                
                log_name = f"{source_name}" + (f" ({source_id})" if source_id else "")
                
                self._log_info(StepResult(
                    step_name=f"加载数据源: {log_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在从 {source_path} 加载数据..."
                ))
                
                if not os.path.exists(source_path):
                    raise FileNotFoundError(f"数据源文件不存在: {source_path}")
                
                if source_type.lower() == 'csv':
                    if skip_invalid_rows:
                        try:
                            df = pd.read_csv(
                                source_path,
                                encoding=encoding,
                                delimiter=delimiter,
                                on_bad_lines='skip',
                                keep_default_na=True
                            )
                        except Exception as e:
                            self._log_warning(StepResult(
                                step_name=f"加载数据源: {log_name}",
                                status=StepStatus.RUNNING,
                                message=f"遇到解析错误，尝试跳过坏行: {str(e)}"
                            ))
                            df = pd.read_csv(
                                source_path,
                                encoding=encoding,
                                delimiter=delimiter,
                                on_bad_lines='warn',
                                keep_default_na=True
                            )
                    else:
                        df = pd.read_csv(
                            source_path,
                            encoding=encoding,
                            delimiter=delimiter,
                            keep_default_na=True
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
                    step_name=f"加载数据源: {log_name}",
                    status=StepStatus.SUCCESS,
                    message=f"成功加载 {len(df)} 行数据，列类型: {dict(df.dtypes)}",
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
    
    def filter_by_date_range(self) -> bool:
        step_result = StepResult(
            step_name="按日期范围过滤数据",
            status=StepStatus.RUNNING,
            message="正在按日期范围过滤数据...",
            start_time=datetime.now()
        )
        
        try:
            wr_config = self.config['weekly_report'].get('date_range', {})
            enabled = wr_config.get('enabled', True)
            
            if not enabled:
                step_result.status = StepStatus.SKIPPED
                step_result.message = "日期范围过滤已禁用"
                step_result.end_time = datetime.now()
                self._log_info(step_result)
                return True
            
            if not self.week_start_date or not self.week_end_date:
                raise ValueError("周范围未计算，请先调用 calculate_week_range")
            
            date_field = wr_config.get('date_field', '订单日期')
            source_table = wr_config.get('source_table', '销售明细')
            
            if source_table not in self.data_tables:
                raise ValueError(f"数据源表 {source_table} 不存在")
            
            df = self.data_tables[source_table].copy()
            original_count = len(df)
            
            if date_field not in df.columns:
                raise ValueError(f"日期字段 {date_field} 不存在于表 {source_table}")
            
            self._log_info(StepResult(
                step_name="按日期范围过滤数据",
                status=StepStatus.RUNNING,
                message=f"日期字段原始类型: {df[date_field].dtype}"
            ))
            
            parsed_dates = pd.to_datetime(df[date_field], errors='coerce')
            na_count = parsed_dates.isna().sum()
            
            if na_count > 0:
                self._log_warning(StepResult(
                    step_name="按日期范围过滤数据",
                    status=StepStatus.RUNNING,
                    message=f"警告: {na_count} 行日期无法解析，将被过滤"
                ))
            
            df['_parsed_date'] = parsed_dates
            
            mask = (
                (df['_parsed_date'] >= self.week_start_date) & 
                (df['_parsed_date'] <= self.week_end_date)
            )
            
            filtered_df = df.loc[mask].copy()
            
            self._log_info(StepResult(
                step_name="按日期范围过滤数据",
                status=StepStatus.RUNNING,
                message=f"保持日期列为 datetime 类型，不转字符串"
            ))
            
            if '_parsed_date' in filtered_df.columns:
                if date_field in filtered_df.columns:
                    filtered_df[date_field] = filtered_df['_parsed_date']
                filtered_df = filtered_df.drop(columns=['_parsed_date'])
            
            self.data_tables[source_table] = filtered_df
            
            filtered_count = len(filtered_df)
            step_result.row_count = filtered_count
            step_result.message = f"日期过滤完成: 原始 {original_count} 行，过滤后 {filtered_count} 行，过滤掉 {original_count - filtered_count} 行"
            step_result.status = StepStatus.SUCCESS
            step_result.end_time = datetime.now()
            
            self._log_info(step_result)
            
            if filtered_count == 0:
                self._log_warning(StepResult(
                    step_name="日期过滤警告",
                    status=StepStatus.SUCCESS,
                    message=f"警告: 日期范围 {self.week_start_date.date()} 至 {self.week_end_date.date()} 内没有数据"
                ))
            
            return True
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"日期范围过滤失败: {str(e)}"
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
            keep_datetime = params.get('keep_datetime', False)
            
            def parse_date(val):
                if pd.isna(val) or val == '':
                    return pd.NaT
                for fmt in input_formats:
                    try:
                        return pd.to_datetime(val, format=fmt)
                    except (ValueError, TypeError):
                        continue
                return pd.NaT
            
            if not pd.api.types.is_datetime64_any_dtype(df[field]):
                df[field] = df[field].apply(parse_date)
            
            if not keep_datetime:
                df[field] = df[field].dt.strftime(output_format)
                self._log_debug(StepResult(
                    step_name=f"数据清洗: {source_name}",
                    status=StepStatus.SUCCESS,
                    message=f"字段 {field}: 日期格式化为字符串 {output_format}"
                ))
            else:
                self._log_debug(StepResult(
                    step_name=f"数据清洗: {source_name}",
                    status=StepStatus.SUCCESS,
                    message=f"字段 {field}: 保持 datetime 类型"
                ))
            
        elif action == 'normalize_category':
            mapping = params.get('mapping', {})
            default = params.get('default')
            
            def normalize_val(x):
                if pd.isna(x):
                    return default
                x_str = str(x).strip()
                if x_str in mapping:
                    return mapping[x_str]
                if str(x) in mapping:
                    return mapping[str(x)]
                return default if default is not None else x
            
            df[field] = df[field].apply(normalize_val)
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
                
                new_columns = {}
                columns_to_drop = []
                
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
                        if to_field in df.columns and to_field not in columns_to_drop:
                            self._log_warning(StepResult(
                                step_name=f"字段映射: {source_name}",
                                status=StepStatus.RUNNING,
                                message=f"目标字段 {to_field} 已存在，将被覆盖"
                            ))
                        new_columns[to_field] = df[from_field]
                        columns_to_drop.append(from_field)
                    
                    self._log_debug(StepResult(
                        step_name=f"字段映射: {source_name}",
                        status=StepStatus.SUCCESS,
                        message=f"字段映射: {from_field} -> {to_field}"
                    ))
                
                for col_name, col_data in new_columns.items():
                    df[col_name] = col_data
                
                if columns_to_drop:
                    df = df.drop(columns=columns_to_drop)
                
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
                
                suffixes_config = join_config.get('suffixes', {'left': '_left', 'right': '_right'})
                left_suffix = suffixes_config.get('left', '_left')
                right_suffix = suffixes_config.get('right', '_right')
                
                conflict_resolution = join_config.get('conflict_resolution', {'mode': 'keep_both'})
                conflict_mode = conflict_resolution.get('mode', 'keep_both')
                
                self._log_info(StepResult(
                    step_name=f"表间关联: {join_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在关联 {left_table} (左) 和 {right_table} (右)，连接类型: {join_type}"
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
                
                self._log_info(StepResult(
                    step_name=f"表间关联: {join_name}",
                    status=StepStatus.RUNNING,
                    message=f"关联键: 左表 {left_keys} -> 右表 {right_keys}"
                ))
                
                left_cols = set(left_df.columns)
                right_cols = set(right_df.columns)
                
                same_join_keys = {lk for lk, rk in zip(left_keys, right_keys) if lk == rk}
                
                all_cols = left_cols & right_cols
                conflicting_cols = all_cols - same_join_keys
                
                if conflicting_cols:
                    self._log_warning(StepResult(
                        step_name=f"表间关联: {join_name}",
                        status=StepStatus.RUNNING,
                        message=f"检测到冲突列: {conflicting_cols}（同名列但不是同一关联键）"
                    ))
                    self._log_warning(StepResult(
                        step_name=f"表间关联: {join_name}",
                        status=StepStatus.RUNNING,
                        message=f"同一关联键: {same_join_keys}（这些不会冲突，merge后只保留一列）"
                    ))
                else:
                    self._log_info(StepResult(
                        step_name=f"表间关联: {join_name}",
                        status=StepStatus.RUNNING,
                        message=f"无冲突列，所有同名列都是关联键: {same_join_keys}"
                    ))
                
                if conflict_mode == 'keep_both':
                    self._log_info(StepResult(
                        step_name=f"表间关联: {join_name}",
                        status=StepStatus.RUNNING,
                        message=f"模式: keep_both - 保留冲突列，使用后缀区分 - 左={left_suffix}, 右={right_suffix}"
                    ))
                    
                    result_df = pd.merge(
                        left_df,
                        right_df,
                        left_on=left_keys,
                        right_on=right_keys,
                        how=join_type,
                        suffixes=(left_suffix, right_suffix)
                    )
                    
                    for lk, rk in zip(left_keys, right_keys):
                        if lk != rk and rk in result_df.columns:
                            self._log_debug(StepResult(
                                step_name=f"表间关联: {join_name}",
                                status=StepStatus.SUCCESS,
                                message=f"保留右表关联键列: {rk}（与左表关联键 {lk} 不同名）"
                            ))
                
                elif conflict_mode == 'left_priority':
                    self._log_info(StepResult(
                        step_name=f"表间关联: {join_name}",
                        status=StepStatus.RUNNING,
                        message=f"模式: left_priority - 冲突列保留左表值，右表冲突列重命名为 {right_suffix} 后缀"
                    ))
                    
                    right_df_to_merge = right_df.copy()
                    
                    right_keys_original = right_keys.copy()
                    
                    for col in conflicting_cols:
                        if col in right_df_to_merge.columns:
                            new_col_name = f"{col}{right_suffix}"
                            self._log_debug(StepResult(
                                step_name=f"表间关联: {join_name}",
                                status=StepStatus.RUNNING,
                                message=f"右表冲突列重命名: {col} -> {new_col_name}"
                            ))
                            right_df_to_merge = right_df_to_merge.rename(columns={col: new_col_name})
                            
                            if col in right_keys:
                                idx = right_keys.index(col)
                                right_keys[idx] = new_col_name
                    
                    self._log_debug(StepResult(
                        step_name=f"表间关联: {join_name}",
                        status=StepStatus.RUNNING,
                        message=f"调整后关联键: 左表 {left_keys} -> 右表 {right_keys}"
                    ))
                    
                    result_df = pd.merge(
                        left_df,
                        right_df_to_merge,
                        left_on=left_keys,
                        right_on=right_keys,
                        how=join_type,
                        suffixes=('', '')
                    )
                
                elif conflict_mode == 'right_priority':
                    self._log_info(StepResult(
                        step_name=f"表间关联: {join_name}",
                        status=StepStatus.RUNNING,
                        message=f"模式: right_priority - 冲突列保留右表值，左表冲突列重命名为 {left_suffix} 后缀"
                    ))
                    
                    left_df_to_merge = left_df.copy()
                    
                    left_keys_original = left_keys.copy()
                    
                    for col in conflicting_cols:
                        if col in left_df_to_merge.columns:
                            new_col_name = f"{col}{left_suffix}"
                            self._log_debug(StepResult(
                                step_name=f"表间关联: {join_name}",
                                status=StepStatus.RUNNING,
                                message=f"左表冲突列重命名: {col} -> {new_col_name}"
                            ))
                            left_df_to_merge = left_df_to_merge.rename(columns={col: new_col_name})
                            
                            if col in left_keys:
                                idx = left_keys.index(col)
                                left_keys[idx] = new_col_name
                    
                    self._log_debug(StepResult(
                        step_name=f"表间关联: {join_name}",
                        status=StepStatus.RUNNING,
                        message=f"调整后关联键: 左表 {left_keys} -> 右表 {right_keys}"
                    ))
                    
                    result_df = pd.merge(
                        left_df_to_merge,
                        right_df,
                        left_on=left_keys,
                        right_on=right_keys,
                        how=join_type,
                        suffixes=('', '')
                    )
                
                else:
                    raise ValueError(f"不支持的冲突处理模式: {conflict_mode}")
                
                self.data_tables[output_table] = result_df
                
                self._log_info(StepResult(
                    step_name=f"表间关联: {join_name}",
                    status=StepStatus.SUCCESS,
                    message=f"关联完成，生成 {output_table}，共 {len(result_df)} 行数据",
                    row_count=len(result_df)
                ))
                
                final_cols = list(result_df.columns)
                self._log_debug(StepResult(
                    step_name=f"表间关联: {join_name}",
                    status=StepStatus.SUCCESS,
                    message=f"输出表列: {final_cols}"
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
            
            self.metrics_id_map.clear()
            
            for calc_config in metrics_config:
                calc_name = calc_config.get('name')
                calc_id = calc_config.get('id')
                source_table = calc_config.get('source_table')
                group_by = calc_config.get('group_by', [])
                metrics = calc_config.get('metrics', [])
                
                log_name = f"{calc_name}" + (f" ({calc_id})" if calc_id else "")
                
                self._log_info(StepResult(
                    step_name=f"指标计算: {log_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在计算 {len(metrics)} 个指标..."
                ))
                
                if source_table not in self.data_tables:
                    raise ValueError(f"源表 {source_table} 不存在")
                
                df = self.data_tables[source_table].copy()
                
                if group_by:
                    result_df = self._calculate_grouped_metrics(df, group_by, metrics, log_name)
                else:
                    result_df = self._calculate_overall_metrics(df, metrics, log_name)
                
                self.metrics_results[calc_name] = result_df
                if calc_id:
                    self.metrics_id_map[calc_id] = calc_name
                
                self._log_info(StepResult(
                    step_name=f"指标计算: {log_name}",
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
                output_id = output_config.get('id')
                source_type = output_config.get('source_type', 'metrics')
                source_id = output_config.get('source_id')
                source_name = output_config.get('source_name')
                output_type = output_config.get('type', 'csv')
                output_path_template = output_config.get('path')
                encoding = output_config.get('encoding', 'utf-8')
                delimiter = output_config.get('delimiter', ',')
                include_timestamp = output_config.get('include_timestamp', False)
                include_week_info = output_config.get('include_week_info', False)
                
                log_name = f"{output_name}" + (f" ({output_id})" if output_id else "")
                
                self._log_info(StepResult(
                    step_name=f"生成输出: {log_name}",
                    status=StepStatus.RUNNING,
                    message=f"正在生成输出文件，源类型: {source_type}, 源ID: {source_id}"
                ))
                
                df = self._get_source_data(source_type, source_id, source_name, log_name)
                
                if include_week_info and self.week_info:
                    existing_cols = set(df.columns)
                    conflicting_week_cols = [k for k in self.week_info.keys() if k in existing_cols]
                    
                    if conflicting_week_cols:
                        self._log_warning(StepResult(
                            step_name=f"生成输出: {log_name}",
                            status=StepStatus.RUNNING,
                            message=f"检测到周信息列名冲突: {conflicting_week_cols}，将覆盖这些列"
                        ))
                    
                    for key, value in reversed(self.week_info.items()):
                        if key in df.columns:
                            df = df.drop(columns=[key])
                        df.insert(0, key, value)
                    
                    self._log_debug(StepResult(
                        step_name=f"生成输出: {log_name}",
                        status=StepStatus.SUCCESS,
                        message=f"已添加周信息列: {list(self.week_info.keys())}"
                    ))
                
                output_path = self._resolve_path(output_path_template)
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
                    step_name=f"生成输出: {log_name}",
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
    
    def _get_source_data(self, source_type: str, source_id: Optional[str], 
                         source_name: Optional[str], log_name: str) -> pd.DataFrame:
        if source_type == 'metrics':
            if source_id and source_id in self.metrics_id_map:
                metrics_name = self.metrics_id_map[source_id]
                if metrics_name in self.metrics_results:
                    return self.metrics_results[metrics_name].copy()
            
            if source_name:
                if source_name in self.metrics_results:
                    return self.metrics_results[source_name].copy()
            
            if source_id:
                for name, df in self.metrics_results.items():
                    if name == source_id:
                        return df.copy()
            
            raise ValueError(
                f"未找到指标数据源: source_id={source_id}, source_name={source_name}\n"
                f"可用ID: {list(self.metrics_id_map.keys())}\n"
                f"可用名称: {list(self.metrics_results.keys())}"
            )
        
        elif source_type == 'table':
            if source_name and source_name in self.data_tables:
                return self.data_tables[source_name].copy()
            
            if source_id and source_id in self.data_tables:
                return self.data_tables[source_id].copy()
            
            raise ValueError(
                f"未找到表数据源: source_id={source_id}, source_name={source_name}\n"
                f"可用表: {list(self.data_tables.keys())}"
            )
        
        else:
            raise ValueError(f"不支持的源类型: {source_type}")
    
    def save_results(self) -> bool:
        step_result = StepResult(
            step_name="保存运行结果",
            status=StepStatus.RUNNING,
            message="正在保存运行结果...",
            start_time=datetime.now()
        )
        
        try:
            serializer = ResultsSerializer(self._get_results_dir())
            success = serializer.save(self)
            
            if success:
                step_result.status = StepStatus.SUCCESS
                step_result.message = f"运行结果已保存到: {self._get_results_dir()}"
            else:
                step_result.status = StepStatus.FAILED
                step_result.message = "保存运行结果失败"
            
            step_result.end_time = datetime.now()
            self._log_info(step_result)
            return success
            
        except Exception as e:
            step_result.status = StepStatus.FAILED
            step_result.message = f"保存运行结果失败: {str(e)}"
            step_result.error_details = traceback.format_exc()
            step_result.end_time = datetime.now()
            self._log_error(step_result)
            return False
    
    def load_results(self) -> bool:
        print(f"正在从 {self._get_results_dir()} 加载之前的运行结果...")
        
        serializer = ResultsSerializer(self._get_results_dir())
        return serializer.load(self)
    
    def run(self) -> Tuple[bool, 'WeeklyReportGenerator']:
        self._log_info(StepResult(
            step_name="周报生成流程",
            status=StepStatus.RUNNING,
            message=f"开始执行周报数据生成流程，时间戳: {self.timestamp}"
        ))
        
        steps = [
            ("加载配置文件", self.load_config),
            ("初始化日志系统", self.setup_logging),
            ("计算周范围", self.calculate_week_range),
            ("加载数据源", self.load_data_sources),
            ("按日期范围过滤", self.filter_by_date_range),
            ("数据清洗", self.apply_cleansing_rules),
            ("字段映射", self.apply_field_mapping),
            ("表间关联", self.perform_table_joins),
            ("指标计算", self.calculate_metrics),
            ("生成输出", self.generate_outputs),
            ("保存运行结果", self.save_results),
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
                    return False, self
                else:
                    self._log_warning(StepResult(
                        step_name="流程控制",
                        status=StepStatus.SKIPPED,
                        message=f"步骤 {step_name} 失败，继续执行后续步骤"
                    ))
        
        self._has_run = True
        
        self._log_info(StepResult(
            step_name="周报生成流程",
            status=StepStatus.SUCCESS,
            message="周报数据生成流程执行完成！"
        ))
        
        return True, self
    
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
    
    def validate_with_expected(self, expected_results_path: str) -> Dict[str, Any]:
        validation_result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'overall_status': 'PENDING',
            'checks': [],
            'summary': {
                'pass_count': 0,
                'fail_count': 0,
                'skip_count': 0,
                'error_count': 0
            }
        }
        
        try:
            expected_path = self.generator._resolve_path(expected_results_path)
            
            with open(expected_path, 'r', encoding='utf-8') as f:
                expected = yaml.safe_load(f)
            
            validations = expected.get('validations', {})
            if not validations:
                print("警告: 未定义验证项")
                validation_result['overall_status'] = 'SKIP'
                return validation_result
            
            for check_name, check_config in validations.items():
                check_result = self._perform_check(check_name, check_config)
                validation_result['checks'].append(check_result)
                
                status = check_result.get('status', 'UNKNOWN')
                if status == 'PASS':
                    validation_result['summary']['pass_count'] += 1
                elif status == 'FAIL':
                    validation_result['summary']['fail_count'] += 1
                elif status == 'SKIP':
                    validation_result['summary']['skip_count'] += 1
                elif status == 'ERROR':
                    validation_result['summary']['error_count'] += 1
            
            if validation_result['summary']['fail_count'] > 0 or validation_result['summary']['error_count'] > 0:
                validation_result['overall_status'] = 'FAIL'
            elif validation_result['summary']['skip_count'] > 0 and validation_result['summary']['pass_count'] == 0:
                validation_result['overall_status'] = 'WARNING'
            else:
                validation_result['overall_status'] = 'PASS'
            
            return validation_result
            
        except Exception as e:
            validation_result['overall_status'] = 'ERROR'
            validation_result['error'] = str(e)
            validation_result['traceback'] = traceback.format_exc()
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
            source_type = check_config.get('source_type', 'auto')
            field = check_config.get('field')
            expected_value = check_config.get('expected')
            
            df = None
            
            if source_type == 'metrics' or source_type == 'auto':
                if source in self.generator.metrics_results:
                    df = self.generator.metrics_results[source]
                elif source in self.generator.metrics_id_map:
                    metrics_name = self.generator.metrics_id_map[source]
                    if metrics_name in self.generator.metrics_results:
                        df = self.generator.metrics_results[metrics_name]
            
            if df is None and (source_type == 'table' or source_type == 'auto'):
                if source in self.generator.data_tables:
                    df = self.generator.data_tables[source]
            
            if df is None:
                result['status'] = 'SKIP'
                result['message'] = f"数据源 {source} 不存在（已搜索metrics和table）"
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
                    result['message'] = f"字段 {field} 不存在于表中，可用列: {list(df.columns)}"
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
                    result['message'] = f"字段 {field} 不存在于表中，可用列: {list(df.columns)}"
                    return result
                
                row_index = check_config.get('row_index', 0)
                if len(df) <= row_index:
                    result['status'] = 'SKIP'
                    result['message'] = f"行索引 {row_index} 超出范围，表只有 {len(df)} 行"
                    return result
                
                actual = df.iloc[row_index][field]
                result['expected'] = expected_value
                result['actual'] = actual
                
                if isinstance(expected_value, (int, float)) and isinstance(actual, (int, float)):
                    if abs(actual - expected_value) < 0.01:
                        result['status'] = 'PASS'
                        result['message'] = f"值检查通过: {actual}"
                    else:
                        result['status'] = 'FAIL'
                        result['message'] = f"值不匹配: 预期 {expected_value}, 实际 {actual}"
                else:
                    if actual == expected_value:
                        result['status'] = 'PASS'
                        result['message'] = f"值检查通过: {actual}"
                    else:
                        result['status'] = 'FAIL'
                        result['message'] = f"值不匹配: 预期 {expected_value}, 实际 {actual}"
            
            elif check_type == 'contains':
                if field not in df.columns:
                    result['status'] = 'SKIP'
                    result['message'] = f"字段 {field} 不存在于表中，可用列: {list(df.columns)}"
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
            
            elif check_type == 'column_exists':
                exists = field in df.columns
                result['expected'] = f"列 {field} 存在"
                result['actual'] = "存在" if exists else "不存在"
                
                if exists:
                    result['status'] = 'PASS'
                    result['message'] = f"列检查通过: {field} 存在"
                else:
                    result['status'] = 'FAIL'
                    result['message'] = f"列检查失败: {field} 不存在，可用列: {list(df.columns)}"
            
            else:
                result['status'] = 'SKIP'
                result['message'] = f"不支持的检查类型: {check_type}"
            
            return result
            
        except Exception as e:
            result['status'] = 'ERROR'
            result['message'] = str(e)
            result['traceback'] = traceback.format_exc()
            return result


def main():
    parser = argparse.ArgumentParser(
        description='周报数据自动化生成工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 使用默认配置运行并保存结果
  python weekly_report_generator.py
  
  # 指定报告日期（自动计算该日期所在周）
  python weekly_report_generator.py -d 2025-04-18
  
  # 自定义日期范围
  python weekly_report_generator.py --start-date 2025-04-14 --end-date 2025-04-20
  
  # 运行并自动验证结果
  python weekly_report_generator.py -m both
  
  # 仅验证（读取上次运行的结果）
  python weekly_report_generator.py -m validate
  
  # 指定周一为周起始日
  python weekly_report_generator.py -w 1
  
  # 指定配置文件路径
  python weekly_report_generator.py -c /path/to/config.yaml
        '''
    )
    
    parser.add_argument('-c', '--config', default='config.yaml', 
                       help='配置文件路径 (默认: config.yaml)')
    parser.add_argument('-m', '--mode', choices=['run', 'validate', 'both'], default='run',
                       help='运行模式: run(运行并保存结果), validate(仅验证，读取上次结果), both(运行并验证) (默认: run)')
    parser.add_argument('-e', '--expected', default='expected_results.yaml', 
                       help='预期结果验证文件路径 (默认: expected_results.yaml)')
    
    date_group = parser.add_argument_group('日期范围选项')
    date_group.add_argument('-d', '--report-date', 
                           help='报告日期 (格式: YYYY-MM-DD)，自动计算该日期所在周')
    date_group.add_argument('-w', '--week-start-day', type=int, choices=[0, 1, 2, 3, 4, 5, 6],
                           help='周起始日: 0=周日, 1=周一, ..., 6=周六 (覆盖配置文件)')
    date_group.add_argument('--start-date', 
                           help='自定义开始日期 (格式: YYYY-MM-DD)，需与 --end-date 一起使用')
    date_group.add_argument('--end-date', 
                           help='自定义结束日期 (格式: YYYY-MM-DD)，需与 --start-date 一起使用')
    
    args = parser.parse_args()
    
    if (args.start_date and not args.end_date) or (args.end_date and not args.start_date):
        print("错误: --start-date 和 --end-date 必须同时使用")
        sys.exit(1)
    
    generator = WeeklyReportGenerator(
        config_path=args.config,
        report_date=args.report_date,
        week_start_day=args.week_start_day,
        custom_start_date=args.start_date,
        custom_end_date=args.end_date
    )
    
    if args.mode == 'run':
        success, generator_instance = generator.run()
        
        if not success:
            print("\n流程执行失败，请检查日志获取详细信息。")
            sys.exit(1)
        
        print(f"\n运行完成！结果已保存到: {generator._get_results_dir()}")
        print("\n如需验证结果，可运行: python weekly_report_generator.py -m validate")
    
    elif args.mode == 'both':
        success, generator_instance = generator.run()
        
        if not success:
            print("\n流程执行失败，请检查日志获取详细信息。")
            sys.exit(1)
        
        print("\n" + "="*60)
        print("开始自动验证结果...")
        print("="*60)
        
        validator = WeeklyReportValidator(generator_instance)
        validation_result = validator.validate_with_expected(args.expected)
        
        summary = validation_result.get('summary', {})
        print(f"\n验证结果: {validation_result['overall_status']}")
        print(f"  通过: {summary.get('pass_count', 0)}, 失败: {summary.get('fail_count', 0)}, 跳过: {summary.get('skip_count', 0)}, 错误: {summary.get('error_count', 0)}")
        print("-"*60)
        
        for check in validation_result.get('checks', []):
            status = check.get('status', 'UNKNOWN')
            if status == 'PASS':
                status_icon = '✓'
                status_color = '\033[92m'
            elif status == 'FAIL':
                status_icon = '✗'
                status_color = '\033[91m'
            elif status == 'SKIP':
                status_icon = '⚪'
                status_color = '\033[93m'
            elif status == 'ERROR':
                status_icon = '✕'
                status_color = '\033[95m'
            else:
                status_icon = '?'
                status_color = '\033[0m'
            
            reset_color = '\033[0m'
            print(f"  {status_color}{status_icon}{reset_color} {check['check_name']}: {check['message']}")
            
            if status in ['FAIL', 'ERROR'] and check.get('expected') is not None:
                print(f"      预期: {check['expected']}")
                print(f"      实际: {check['actual']}")
        
        if validation_result.get('error'):
            print(f"\n错误详情: {validation_result['error']}")
            if validation_result.get('traceback'):
                print(f"堆栈: {validation_result['traceback']}")
        
        if validation_result['overall_status'] == 'FAIL':
            print("\n验证失败！")
            sys.exit(1)
        elif validation_result['overall_status'] == 'WARNING':
            print("\n验证有警告（所有检查跳过或无有效检查）")
        else:
            print("\n✓ 所有验证通过！")
    
    elif args.mode == 'validate':
        generator.load_config()
        generator.setup_logging()
        
        if not generator.load_results():
            print("\n错误: 无法加载之前的运行结果。请先运行 -m run 或 -m both")
            sys.exit(1)
        
        print("\n" + "="*60)
        print("开始验证结果（从上次运行加载）...")
        print("="*60)
        
        validator = WeeklyReportValidator(generator)
        validation_result = validator.validate_with_expected(args.expected)
        
        summary = validation_result.get('summary', {})
        print(f"\n验证结果: {validation_result['overall_status']}")
        print(f"  通过: {summary.get('pass_count', 0)}, 失败: {summary.get('fail_count', 0)}, 跳过: {summary.get('skip_count', 0)}, 错误: {summary.get('error_count', 0)}")
        print("-"*60)
        
        for check in validation_result.get('checks', []):
            status = check.get('status', 'UNKNOWN')
            if status == 'PASS':
                status_icon = '✓'
            elif status == 'FAIL':
                status_icon = '✗'
            elif status == 'SKIP':
                status_icon = '⚪'
            else:
                status_icon = '?'
            print(f"  {status_icon} {check['check_name']}: {check['message']}")
            
            if status == 'FAIL' and check.get('expected') is not None:
                print(f"      预期: {check['expected']}")
                print(f"      实际: {check['actual']}")
        
        if validation_result['overall_status'] in ['FAIL', 'ERROR']:
            print("\n验证失败！")
            sys.exit(1)
    
    print("\n" + "="*60)
    print("所有操作完成！")
    print("="*60)


if __name__ == '__main__':
    main()

