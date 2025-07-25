# --- START OF FILE app/config.py ---
import json


def load_config():
    """
    加载并验证配置文件。
    """
    config_file_paths = ['config.json', 'config/config.json']
    config_data = None
    loaded_path = ""
    for path in config_file_paths:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            loaded_path = path
            break
        except FileNotFoundError:
            continue

    if config_data is None:
        raise FileNotFoundError(f"找不到配置文件，尝试了 {', '.join(config_file_paths)}")

    try:
        # 在这里可以添加更多的配置验证逻辑
        assert 'app_settings' in config_data
        assert 'exchange' in config_data['app_settings']
        return config_data
    except json.JSONDecodeError:
        raise ValueError(f"配置文件 '{loaded_path}' 格式不正确。")
    except AssertionError:
        raise ValueError(f"配置文件 '{loaded_path}' 缺少必要的配置项。")
# --- END OF FILE app/config.py ---