"""state 功能域门面。

当前为了保证实盘行为与 refactored 单文件版本完全一致，具体实现仍由 core_engine 提供。
后续迁移时只需要把对应实现从 core_engine 移入本模块，不改变外部调用接口。
"""
from . import core_engine

__all__ = ["core_engine"]
