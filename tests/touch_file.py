import os
import time

def create_and_verify_file(filepath: str):
    """创建并验证测试文件"""
    comm_file = filepath
    # 确保文件不存在
    if os.path.exists(comm_file):
        os.remove(comm_file)
    
    # 创建文件并写入内容
    with open(comm_file, 'w') as f:
        f.write("STARTED")
    
    # 验证文件
    if os.path.exists(comm_file):
        print(f"✅ 测试成功，文件已创建: {comm_file}")
        with open(comm_file) as f:
            print(f"文件内容: {f.read()}")
    else:
        print("❌ 文件未创建")

    # 清理（可选）
    # os.remove(comm_file)

if __name__ == '__main__':
    comm_file = os.path.join(os.getcwd(), "proc_comm_test.txt")
    create_and_verify_file(comm_file)