import os
import shutil
from pathlib import Path
from PIL import Image
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_image_files(folder_path, extensions=None):
    """获取指定文件夹中的所有图片文件"""
    if extensions is None:
        extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff', '.webp', '.jfif'}
    
    image_files = []
    for ext in extensions:
        image_files.extend(Path(folder_path).glob(f'*{ext}'))
        image_files.extend(Path(folder_path).glob(f'*{ext.upper()}'))
    
    return image_files

def calculate_aspect_ratio(width, height):
    """计算图片的宽高比"""
    if height == 0:
        return float('inf')
    return width / height

def classify_image_by_aspect_ratio(image_path, pc_threshold=1.2, sj_threshold=0.8):
    """
    根据宽高比分类图片
    
    Args:
        image_path: 图片路径
        pc_threshold: PC分类阈值，宽高比大于此值视为PC图片
        sj_threshold: SJ分类阈值，宽高比小于此值视为SJ图片
        
    Returns:
        'PC', 'SJ' 或 'Unknown'
    """
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            aspect_ratio = calculate_aspect_ratio(width, height)
            
            logger.debug(f"图片: {image_path.name}, 尺寸: {width}x{height}, 宽高比: {aspect_ratio:.2f}")
            
            # 根据宽高比分类
            if aspect_ratio > pc_threshold:
                return 'PC'  # 宽屏，适合电脑查看
            elif aspect_ratio < sj_threshold:
                return 'SJ'  # 竖屏，适合手机查看
            else:
                # 接近正方形的图片，可以根据需求进一步处理
                # 这里我们根据哪个方向更接近标准设备比例来分类
                pc_diff = abs(aspect_ratio - 16/9)  # 电脑常见比例
                sj_diff = abs(aspect_ratio - 9/16)  # 手机常见比例
                
                if pc_diff < sj_diff:
                    return 'PC'
                else:
                    return 'SJ'
                    
    except Exception as e:
        logger.error(f"无法处理图片 {image_path.name}: {e}")
        return 'Unknown'

def create_target_folders(source_folder):
    """创建目标文件夹"""
    pc_folder = Path(source_folder) / 'PC'
    sj_folder = Path(source_folder) / 'SJ'
    
    pc_folder.mkdir(exist_ok=True)
    sj_folder.mkdir(exist_ok=True)
    
    return pc_folder, sj_folder

def copy_or_move_file(source_file, target_folder, move=False):
    """复制或移动文件到目标文件夹，处理文件名冲突"""
    target_path = target_folder / source_file.name
    
    # 如果目标文件已存在，添加序号
    counter = 1
    while target_path.exists():
        name_parts = source_file.stem.split('_')
        if name_parts[-1].isdigit() and len(name_parts) > 1:
            base_name = '_'.join(name_parts[:-1])
        else:
            base_name = source_file.stem
        
        new_name = f"{base_name}_{counter}{source_file.suffix}"
        target_path = target_folder / new_name
        counter += 1
    
    try:
        if move:
            shutil.move(str(source_file), str(target_path))
            logger.info(f"已移动: {source_file.name} -> {target_folder.name}/")
        else:
            shutil.copy2(str(source_file), str(target_path))
            logger.info(f"已复制: {source_file.name} -> {target_folder.name}/")
        return True
    except Exception as e:
        logger.error(f"操作失败 {source_file.name}: {e}")
        return False

def classify_images(source_folder, move_files=False, pc_threshold=1.2, sj_threshold=0.8):
    """
    主函数：分类图片
    
    Args:
        source_folder: 源文件夹路径
        move_files: 是否移动文件（True=移动，False=复制）
        pc_threshold: PC分类阈值
        sj_threshold: SJ分类阈值
    """
    # 验证源文件夹
    source_path = Path(source_folder)
    if not source_path.exists() or not source_path.is_dir():
        logger.error(f"指定的文件夹不存在或不是有效目录: {source_folder}")
        return False
    
    # 获取图片文件
    image_files = get_image_files(source_folder)
    if not image_files:
        logger.warning(f"在文件夹 {source_folder} 中未找到图片文件")
        return False
    
    logger.info(f"找到 {len(image_files)} 个图片文件")
    
    # 创建目标文件夹
    pc_folder, sj_folder = create_target_folders(source_folder)
    logger.info(f"已创建分类文件夹: PC 和 SJ")
    
    # 统计信息
    stats = {
        'PC': 0,
        'SJ': 0,
        'Unknown': 0,
        'Failed': 0
    }
    
    # 分类处理每个图片
    for image_file in image_files:
        classification = classify_image_by_aspect_ratio(
            image_file, 
            pc_threshold, 
            sj_threshold
        )
        
        if classification == 'PC':
            target_folder = pc_folder
            stats['PC'] += 1
        elif classification == 'SJ':
            target_folder = sj_folder
            stats['SJ'] += 1
        else:
            stats['Unknown'] += 1
            logger.warning(f"无法分类: {image_file.name}")
            continue
        
        # 复制或移动文件
        success = copy_or_move_file(image_file, target_folder, move_files)
        if not success:
            stats['Failed'] += 1
    
    # 输出统计信息
    logger.info("\n" + "="*50)
    logger.info("分类完成！统计结果:")
    logger.info(f"PC (电脑横屏): {stats['PC']} 个")
    logger.info(f"SJ (手机竖屏): {stats['SJ']} 个")
    logger.info(f"未分类: {stats['Unknown']} 个")
    logger.info(f"失败: {stats['Failed']} 个")
    logger.info("="*50)
    
    # 显示分类后的文件夹路径
    logger.info(f"\n分类后的文件位于:")
    logger.info(f"PC文件夹: {pc_folder.absolute()}")
    logger.info(f"SJ文件夹: {sj_folder.absolute()}")
    
    return True

def main():
    """主函数，处理用户交互"""
    print("="*60)
    print("图片分类工具 - 按屏幕比例分类为 PC(电脑) 和 SJ(手机)")
    print("="*60)
    
    # 获取用户输入
    while True:
        source_folder = input("\n请输入要分类的图片文件夹路径: ").strip()
        
        if not source_folder:
            print("错误: 路径不能为空!")
            continue
            
        source_path = Path(source_folder)
        if not source_path.exists():
            print(f"错误: 文件夹 '{source_folder}' 不存在!")
            continue
            
        if not source_path.is_dir():
            print(f"错误: '{source_folder}' 不是有效的文件夹!")
            continue
            
        break
    
    # 询问操作模式
    print("\n请选择操作模式:")
    print("1. 复制文件（保留原文件）")
    print("2. 移动文件（将原文件移动到分类文件夹）")
    
    while True:
        choice = input("请选择 (1 或 2): ").strip()
        if choice in ['1', '2']:
            move_files = (choice == '2')
            break
        print("错误: 请输入 1 或 2!")
    
    # 可选：调整分类阈值
    print("\n可选：调整分类阈值（按回车使用默认值）")
    
    try:
        pc_input = input("PC阈值（宽高比>此值为PC，默认1.2）: ").strip()
        pc_threshold = float(pc_input) if pc_input else 1.2
        
        sj_input = input("SJ阈值（宽高比<此值为SJ，默认0.8）: ").strip()
        sj_threshold = float(sj_input) if sj_input else 0.8
    except ValueError:
        print("输入无效，使用默认阈值")
        pc_threshold = 1.2
        sj_threshold = 0.8
    
    print(f"\n开始处理文件夹: {source_folder}")
    print(f"操作模式: {'移动文件' if move_files else '复制文件'}")
    print(f"分类阈值: PC > {pc_threshold}, SJ < {sj_threshold}")
    print("-"*60)
    
    # 执行分类
    try:
        success = classify_images(
            source_folder=source_folder,
            move_files=move_files,
            pc_threshold=pc_threshold,
            sj_threshold=sj_threshold
        )
        
        if success:
            print("\n✓ 分类完成！")
        else:
            print("\n✗ 分类过程中出现问题")
            
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        print("\n✗ 程序执行出错，请检查错误信息")
    
    input("\n按回车键退出...")

if __name__ == "__main__":
    # 检查必要的库
    try:
        from PIL import Image
    except ImportError:
        print("错误: 需要安装Pillow库")
        print("请运行: pip install pillow")
        input("按回车键退出...")
        exit(1)
    
    main()