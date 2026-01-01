import os
import sys
import hashlib
import shutil
import concurrent.futures
from pathlib import Path
from PIL import Image, ImageChops
import imagehash
import logging
from collections import defaultdict
import json
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('duplicate_cleaner.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class DuplicateImageCleaner:
    def __init__(self, target_folder):
        """初始化清理器"""
        self.target_folder = Path(target_folder).resolve()
        self.supported_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff', '.webp', '.jfif'}
        self.duplicates_found = 0
        self.space_saved = 0  # 字节
        
        # 验证文件夹
        if not self.target_folder.exists():
            raise FileNotFoundError(f"文件夹不存在: {self.target_folder}")
        if not self.target_folder.is_dir():
            raise NotADirectoryError(f"路径不是文件夹: {self.target_folder}")
    
    def get_all_image_files(self):
        """递归获取所有支持的图片文件"""
        image_files = []
        total_count = 0
        
        logger.info("正在扫描图片文件...")
        
        # 递归扫描所有子文件夹
        for root, dirs, files in os.walk(self.target_folder):
            root_path = Path(root)
            
            for file in files:
                file_path = root_path / file
                ext = file_path.suffix.lower()
                
                # 检查文件扩展名
                if ext in self.supported_extensions:
                    try:
                        # 验证文件是否可以打开（确保是有效的图片）
                        with Image.open(file_path) as img:
                            img.verify()  # 验证但不加载
                        image_files.append(file_path)
                    except Exception:
                        logger.warning(f"无法打开或验证图片: {file_path}")
                        continue
        
        logger.info(f"找到 {len(image_files)} 个有效的图片文件")
        return image_files
    
    def calculate_file_hash(self, file_path):
        """计算文件的MD5哈希值"""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                # 分块读取大文件，避免内存问题
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"计算哈希失败 {file_path}: {e}")
            return None
    
    def calculate_image_hash(self, file_path):
        """计算图片的感知哈希（用于相似图片检测）"""
        try:
            with Image.open(file_path) as img:
                # 转换为RGB模式（统一格式）
                if img.mode not in ['RGB', 'RGBA', 'L']:
                    img = img.convert('RGB')
                # 计算感知哈希
                return str(imagehash.average_hash(img))
        except Exception as e:
            logger.error(f"计算图片哈希失败 {file_path}: {e}")
            return None
    
    def get_image_metadata(self, file_path):
        """获取图片的元数据"""
        try:
            with Image.open(file_path) as img:
                width, height = img.size
                file_size = file_path.stat().st_size
                created_time = datetime.fromtimestamp(file_path.stat().st_ctime)
                modified_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                return {
                    'size': file_size,
                    'dimensions': (width, height),
                    'created': created_time,
                    'modified': modified_time,
                    'format': img.format
                }
        except Exception as e:
            logger.error(f"获取元数据失败 {file_path}: {e}")
            return None
    
    def are_images_identical(self, img1_path, img2_path, metadata1, metadata2):
        """检查两个图片是否完全相同"""
        # 快速检查：文件大小和尺寸
        if metadata1['size'] != metadata2['size']:
            return False
        
        if metadata1['dimensions'] != metadata2['dimensions']:
            return False
        
        # 检查文件哈希
        hash1 = self.calculate_file_hash(img1_path)
        hash2 = self.calculate_file_hash(img2_path)
        
        if hash1 and hash2 and hash1 == hash2:
            return True
        
        # 如果哈希不同但尺寸相同，进一步检查像素内容
        try:
            with Image.open(img1_path) as img1, Image.open(img2_path) as img2:
                # 确保图片模式一致
                if img1.mode != img2.mode:
                    img2 = img2.convert(img1.mode)
                
                # 检查尺寸
                if img1.size != img2.size:
                    return False
                
                # 比较像素差异
                diff = ImageChops.difference(img1, img2)
                if diff.getbbox() is None:
                    return True
        except Exception as e:
            logger.error(f"比较图片失败 {img1_path} vs {img2_path}: {e}")
        
        return False
    
    def find_duplicates_by_hash(self, image_files):
        """通过文件哈希查找重复图片"""
        logger.info("正在通过文件哈希查找重复图片...")
        
        hash_groups = defaultdict(list)
        
        # 使用线程池加速哈希计算
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            # 提交所有哈希计算任务
            future_to_file = {executor.submit(self.calculate_file_hash, f): f for f in image_files}
            
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    file_hash = future.result()
                    if file_hash:
                        hash_groups[file_hash].append(file_path)
                except Exception as e:
                    logger.error(f"处理文件失败 {file_path}: {e}")
        
        # 筛选出有重复的组
        duplicate_groups = {h: files for h, files in hash_groups.items() if len(files) > 1}
        
        logger.info(f"通过文件哈希找到 {len(duplicate_groups)} 组重复图片")
        return duplicate_groups
    
    def find_duplicates_by_metadata(self, image_files):
        """通过元数据（尺寸和文件大小）查找可能的重复图片"""
        logger.info("正在通过元数据查找可能的重复图片...")
        
        # 获取所有图片的元数据
        metadata_dict = {}
        for file_path in image_files:
            metadata = self.get_image_metadata(file_path)
            if metadata:
                metadata_dict[file_path] = metadata
        
        # 按尺寸和文件大小分组
        size_groups = defaultdict(list)
        for file_path, metadata in metadata_dict.items():
            key = (metadata['dimensions'], metadata['size'])
            size_groups[key].append(file_path)
        
        # 筛选出有重复的组
        potential_duplicates = {k: files for k, files in size_groups.items() if len(files) > 1}
        
        logger.info(f"通过元数据找到 {len(potential_duplicates)} 组可能的重复图片")
        return potential_duplicates, metadata_dict
    
    def find_duplicate_groups(self, image_files):
        """查找所有重复图片组"""
        # 方法1：通过文件哈希查找完全相同的文件
        hash_duplicates = self.find_duplicates_by_hash(image_files)
        
        # 方法2：通过元数据查找可能的重复
        size_duplicates, metadata_dict = self.find_duplicates_by_metadata(image_files)
        
        # 合并结果，确保不重复计数
        all_duplicate_groups = []
        processed_files = set()
        
        # 首先处理哈希重复组
        for hash_val, files in hash_duplicates.items():
            if not any(f in processed_files for f in files):
                all_duplicate_groups.append({
                    'type': 'exact_hash',
                    'files': files,
                    'count': len(files)
                })
                processed_files.update(files)
        
        # 处理尺寸重复组（排除已处理的文件）
        for (dimensions, size), files in size_duplicates.items():
            # 过滤掉已处理的文件
            remaining_files = [f for f in files if f not in processed_files]
            
            if len(remaining_files) > 1:
                # 在这些文件中进一步检查实际是否重复
                verified_groups = self.verify_size_duplicates(remaining_files, metadata_dict)
                all_duplicate_groups.extend(verified_groups)
                for group in verified_groups:
                    processed_files.update(group['files'])
        
        # 统计信息
        total_duplicates = sum(len(group['files']) - 1 for group in all_duplicate_groups)
        self.duplicates_found = total_duplicates
        
        logger.info(f"总共找到 {total_duplicates} 个重复文件，分布在 {len(all_duplicate_groups)} 个组中")
        return all_duplicate_groups
    
    def verify_size_duplicates(self, files, metadata_dict):
        """验证具有相同尺寸的文件是否真的重复"""
        groups = []
        processed = set()
        
        for i, file1 in enumerate(files):
            if file1 in processed:
                continue
            
            # 为当前文件创建一个新组
            current_group = [file1]
            
            for j, file2 in enumerate(files[i+1:], start=i+1):
                if file2 in processed:
                    continue
                
                # 检查两个图片是否相同
                if self.are_images_identical(
                    file1, file2, 
                    metadata_dict.get(file1), 
                    metadata_dict.get(file2)
                ):
                    current_group.append(file2)
                    processed.add(file2)
            
            if len(current_group) > 1:
                groups.append({
                    'type': 'exact_content',
                    'files': current_group,
                    'count': len(current_group)
                })
                processed.add(file1)
        
        return groups
    
    def select_kept_file(self, file_group):
        """选择要保留的文件（基于创建时间、文件大小等）"""
        if not file_group:
            return None
        
        # 策略：优先保留最清晰的（文件最大的），如果文件大小相同则保留最早的
        file_info = []
        for file_path in file_group:
            try:
                stat = file_path.stat()
                file_info.append({
                    'path': file_path,
                    'size': stat.st_size,
                    'created': stat.st_ctime,
                    'modified': stat.st_mtime
                })
            except Exception as e:
                logger.error(f"无法获取文件信息 {file_path}: {e}")
                file_info.append({
                    'path': file_path,
                    'size': 0,
                    'created': float('inf'),
                    'modified': float('inf')
                })
        
        # 按文件大小降序，创建时间升序排序
        file_info.sort(key=lambda x: (-x['size'], x['created']))
        return file_info[0]['path']
    
    def cleanup_duplicates(self, duplicate_groups, dry_run=True, backup_folder=None):
        """清理重复图片"""
        if dry_run:
            logger.info("=" * 60)
            logger.info("模拟运行模式 - 不会实际删除任何文件")
            logger.info("=" * 60)
        
        # 创建备份文件夹（如果需要）
        if backup_folder and not dry_run:
            backup_path = Path(backup_folder)
            backup_path.mkdir(parents=True, exist_ok=True)
        
        # 统计信息
        cleanup_stats = {
            'total_groups': len(duplicate_groups),
            'total_duplicates': 0,
            'space_to_save': 0,
            'kept_files': [],
            'removed_files': []
        }
        
        # 处理每个重复组
        for idx, group in enumerate(duplicate_groups, 1):
            files = group['files']
            logger.info(f"\n处理重复组 {idx}/{len(duplicate_groups)}:")
            logger.info(f"  类型: {group['type']}, 文件数: {len(files)}")
            
            # 选择要保留的文件
            kept_file = self.select_kept_file(files)
            if not kept_file:
                logger.warning("  无法选择要保留的文件，跳过此组")
                continue
            
            # 记录要删除的文件
            files_to_remove = [f for f in files if f != kept_file]
            
            # 计算可节省的空间
            for file_to_remove in files_to_remove:
                try:
                    file_size = file_to_remove.stat().st_size
                    cleanup_stats['space_to_save'] += file_size
                except Exception:
                    pass
            
            # 记录统计信息
            cleanup_stats['total_duplicates'] += len(files_to_remove)
            cleanup_stats['kept_files'].append(kept_file)
            cleanup_stats['removed_files'].extend(files_to_remove)
            
            # 显示详细信息
            logger.info(f"  保留: {kept_file.name} ({kept_file.parent.name}\\{kept_file.name})")
            
            for file_to_remove in files_to_remove:
                logger.info(f"  删除: {file_to_remove.name} ({file_to_remove.parent.name}\\{file_to_remove.name})")
                
                # 如果不是模拟运行，则执行删除或备份
                if not dry_run:
                    try:
                        if backup_folder:
                            # 创建备份
                            rel_path = file_to_remove.relative_to(self.target_folder)
                            backup_file = backup_path / rel_path
                            backup_file.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(file_to_remove, backup_file)
                        
                        # 删除文件
                        file_to_remove.unlink()
                        logger.debug(f"    已删除: {file_to_remove}")
                    except Exception as e:
                        logger.error(f"    删除失败 {file_to_remove}: {e}")
        
        # 更新全局统计
        self.space_saved = cleanup_stats['space_to_save']
        
        return cleanup_stats
    
    def save_report(self, duplicate_groups, cleanup_stats, report_path=None):
        """保存清理报告"""
        if report_path is None:
            report_path = self.target_folder / f"duplicate_cleanup_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        else:
            report_path = Path(report_path)
        
        # 准备报告数据
        report_data = {
            'scan_date': datetime.now().isoformat(),
            'target_folder': str(self.target_folder),
            'duplicate_groups_count': len(duplicate_groups),
            'total_duplicates_found': self.duplicates_found,
            'space_saved_bytes': self.space_saved,
            'space_saved_mb': round(self.space_saved / (1024 * 1024), 2),
            'space_saved_gb': round(self.space_saved / (1024 * 1024 * 1024), 2),
            'cleanup_stats': cleanup_stats,
            'duplicate_groups': []
        }
        
        # 添加重复组详细信息
        for group in duplicate_groups:
            group_info = {
                'type': group['type'],
                'count': len(group['files']),
                'files': [str(f) for f in group['files']]
            }
            report_data['duplicate_groups'].append(group_info)
        
        # 保存JSON报告
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            logger.info(f"\n详细报告已保存: {report_path}")
        except Exception as e:
            logger.error(f"保存报告失败: {e}")
        
        return report_path
    
    def print_summary(self, cleanup_stats, dry_run=True):
        """打印清理总结"""
        print("\n" + "=" * 60)
        print("重复图片清理完成!")
        print("=" * 60)
        
        if dry_run:
            print("运行模式: 模拟运行（未删除任何文件）")
        else:
            print("运行模式: 实际执行")
        
        print(f"\n扫描文件夹: {self.target_folder}")
        print(f"重复组数: {cleanup_stats['total_groups']}")
        print(f"重复文件数: {cleanup_stats['total_duplicates']}")
        
        space_mb = round(cleanup_stats['space_to_save'] / (1024 * 1024), 2)
        space_gb = round(cleanup_stats['space_to_save'] / (1024 * 1024 * 1024), 2)
        
        print(f"\n可节省空间:")
        print(f"  {cleanup_stats['space_to_save']:,} 字节")
        print(f"  {space_mb:,} MB")
        print(f"  {space_gb:.2f} GB")
        
        if not dry_run:
            print(f"\n实际删除文件: {len(cleanup_stats['removed_files'])} 个")
            print(f"保留文件: {len(cleanup_stats['kept_files'])} 个")
        
        print("\n注意: 请查看日志文件 'duplicate_cleaner.log' 获取详细信息")

def main():
    """主函数"""
    print("=" * 60)
    print("重复图片检测与清理工具")
    print("=" * 60)
    
    # 获取目标文件夹
    while True:
        target_folder = input("\n请输入要处理的图片文件夹路径: ").strip()
        
        if not target_folder:
            print("错误: 路径不能为空!")
            continue
        
        target_path = Path(target_folder)
        if not target_path.exists():
            print(f"错误: 文件夹 '{target_folder}' 不存在!")
            continue
        
        if not target_path.is_dir():
            print(f"错误: '{target_folder}' 不是有效的文件夹!")
            continue
        
        break
    
    # 初始化清理器
    try:
        cleaner = DuplicateImageCleaner(target_folder)
    except Exception as e:
        print(f"初始化失败: {e}")
        input("按回车键退出...")
        return
    
    # 询问运行模式
    print("\n请选择运行模式:")
    print("1. 模拟运行（只检测，不删除）")
    print("2. 实际执行（删除重复文件）")
    
    while True:
        mode = input("请选择 (1 或 2): ").strip()
        if mode in ['1', '2']:
            dry_run = (mode == '1')
            break
        print("错误: 请输入 1 或 2!")
    
    # 询问是否创建备份
    backup_folder = None
    if not dry_run:
        backup_option = input("\n是否创建备份文件夹? (y/n, 默认: n): ").strip().lower()
        if backup_option in ['y', 'yes', '是']:
            backup_folder = cleaner.target_folder / "backup_duplicates"
            print(f"备份将保存在: {backup_folder}")
    
    # 开始处理
    print("\n" + "=" * 60)
    print("开始处理...")
    print("=" * 60)
    
    try:
        # 获取所有图片文件
        image_files = cleaner.get_all_image_files()
        
        if not image_files:
            print("未找到任何图片文件!")
            input("按回车键退出...")
            return
        
        # 查找重复图片
        duplicate_groups = cleaner.find_duplicate_groups(image_files)
        
        if not duplicate_groups:
            print("\n恭喜! 没有找到重复图片!")
            input("按回车键退出...")
            return
        
        # 清理重复图片
        cleanup_stats = cleaner.cleanup_duplicates(
            duplicate_groups, 
            dry_run=dry_run,
            backup_folder=backup_folder
        )
        
        # 保存报告
        report_path = cleaner.save_report(duplicate_groups, cleanup_stats)
        
        # 打印总结
        cleaner.print_summary(cleanup_stats, dry_run)
        
        if dry_run:
            print("\n提示: 要实际删除重复文件，请重新运行程序并选择模式2")
        
    except KeyboardInterrupt:
        print("\n\n用户中断操作")
    except Exception as e:
        logger.error(f"处理过程中发生错误: {e}")
        print(f"\n处理过程中发生错误，请查看日志文件获取详细信息")
    
    input("\n按回车键退出...")

def install_dependencies():
    """安装必要的依赖"""
    import subprocess
    import sys
    
    print("正在检查并安装必要的依赖...")
    
    required_packages = [
        'pillow',
        'imagehash'
    ]
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✓ {package} 已安装")
        except ImportError:
            print(f"正在安装 {package}...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
                print(f"✓ {package} 安装成功")
            except subprocess.CalledProcessError:
                print(f"✗ {package} 安装失败，请手动安装: pip install {package}")
                return False
    
    return True

if __name__ == "__main__":
    # 检查并安装依赖
    try:
        from PIL import Image
        import imagehash
    except ImportError:
        print("缺少必要的依赖库")
        if install_dependencies():
            print("\n依赖安装成功，请重新运行程序")
        else:
            print("\n依赖安装失败，请手动安装:")
            print("pip install pillow imagehash")
        input("按回车键退出...")
        sys.exit(1)
    
    # 运行主程序
    main()