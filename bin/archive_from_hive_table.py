# -*- coding: UTF-8 -*-
# Date Time     : 2019/1/2
# Write By      : adtec(ZENGYU)
# Function Desc :  归档执行主类
# History       : 2019/1/2  ZENGYU     Create
# Remarks       :
import os
import sys

from archive.archive_way import *

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))


def main():
    args = ArchiveData.archive_init()
    # 判断归档模式 执行不同的实现类
    archive = None

    save_mode = args.saveMode
    if save_mode == "1":
        archive = AllArchive()
    elif save_mode == "2":
        archive = AddArchive()
    elif save_mode == "4":
        archive = ChainTransArchive()
    elif save_mode == "5":
        archive = LastAddArchive()
    elif save_mode == "6":
        archive = LastAllArchive()
    if archive:
        archive.run()


if __name__ == '__main__':
    main()
