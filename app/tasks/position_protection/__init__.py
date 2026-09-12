"""模块化入口：当前版本保持与原 position_protection_ws_v2_refactored.py 100% 行为兼容。"""
from .core_engine import protect_positions_main, watch_symbol_position

__all__ = ["protect_positions_main", "watch_symbol_position"]
