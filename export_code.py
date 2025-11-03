# export_code.py (v2 - 自动检测当前目录)

import os
import sys

# --- 用户配置区 START ---

# 1. 排除的目录名：这些目录下的所有内容都将被忽略
EXCLUDE_DIRS = {
    '.git', 'node_modules', 'venv', '__pycache__', 'dist', 'build',
    '.vscode', '.idea', 'env', 'bin', 'lib', 'obj',  # 常用虚拟环境、编译产物和IDE目录
}

# 2. 排除的文件名：这些特定文件将被忽略
EXCLUDE_FILES = {
    'package-lock.json', 'yarn.lock', '.env', '.DS_Store', 'Thumbs.db'
}

# 3. 排除的文件扩展名：这些类型的文件将被忽略 (注意前面的点'.')
EXCLUDE_EXTENSIONS = {
    '.log', '.tmp', '.swp', '.bak', '.zip', '.rar', '.7z',
    # 媒体文件
    '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp',
    '.mp4', '.mov', '.avi', '.mp3', '.wav',
    # 文档和数据
    '.pdf', '.doc', '.docx', '.xls', '.xlsx',
    '.db', '.sqlite3',
    # 编译产物
    '.pyc', '.o', '.so', '.dll', '.exe', '.class'
}


# --- 用户配置区 END ---


def get_file_content(file_path):
    """尝试以UTF-8编码读取文件内容，如果失败则返回提示信息。"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        # 如果UTF-8失败，尝试用系统默认编码
        try:
            with open(file_path, 'r', encoding=sys.getdefaultencoding()) as f:
                return f.read()
        except Exception as e:
            return f"--- [无法读取文件: {e}] ---"


def main():
    """主函数，遍历当前目录并生成整合文件。"""
    project_path = os.getcwd()  # 使用当前工作目录作为项目根目录
    project_name = os.path.basename(project_path)

    # 将输出文件保存在项目目录的外面 (上一级目录)
    output_filename = f"{project_name}_code_for_ai.txt"
    parent_dir = os.path.dirname(project_path)
    output_file_path = os.path.join(parent_dir, output_filename)

    # 动态排除脚本自身和输出文件
    script_name = os.path.basename(__file__)
    EXCLUDE_FILES.add(script_name)
    EXCLUDE_FILES.add(output_filename)  # 避免在同一目录时把自己打包

    print(f"▶️  开始扫描项目: {project_name}")
    print(f"   项目路径: {project_path}")
    print(f"   输出文件将保存在: {output_file_path}\n")

    file_count = 0
    with open(output_file_path, 'w', encoding='utf-8') as outfile:
        outfile.write(f"# 項目 '{project_name}' 的代碼合集\n\n")

        for root, dirs, files in os.walk(project_path, topdown=True):
            # 排除指定目录
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

            for filename in files:
                # 排除指定文件和扩展名
                if filename in EXCLUDE_FILES or os.path.splitext(filename)[1] in EXCLUDE_EXTENSIONS:
                    continue

                file_path = os.path.join(root, filename)
                relative_path = os.path.relpath(file_path, project_path)

                # 使用正斜杠作为路径分隔符，提高跨平台可读性
                formatted_path = relative_path.replace(os.sep, '/')

                outfile.write("=" * 35 + f"  📄 {formatted_path}  " + "=" * 35 + "\n\n")
                outfile.write("```\n")

                content = get_file_content(file_path)
                outfile.write(content.strip() + "\n")

                outfile.write("```\n\n\n")
                file_count += 1

    print(f"✅ 成功！共处理了 {file_count} 个文件。")
    print(f"   所有代码已整合到文件 '{output_file_path}' 中。")
    print("\n下一步：请打开该文件，检查并删除任何敏感信息（如密码、API密钥等），然后将其内容复制给我。")


if __name__ == '__main__':
    main()