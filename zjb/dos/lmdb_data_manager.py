import logging
import os
from contextlib import contextmanager
from enum import Enum
from pickle import dumps, loads
from typing import Any, NamedTuple

import lmdb
from traits.has_traits import HasRequiredTraits
from traits.trait_types import Directory

from zjb.dos.data import Data

from .data_manager import DataManager

logger = logging.getLogger(__name__)

# 定义常量
META_ENV = 'meta.mdb'
META_MAP_SIZE = 1024 ** 2
DATA_ENV = 'data.mdb'
DATA_MAP_SIZE = b'data_map_size'
DEFAULT_DATA_MAP_SIZE = b'\x10\x00\x00'
DATA_MAP_SIZE_LENGTH = 8
MAX_DATA_MAP_SIZE_INCREASE = 1024 ** 3


class _DB(Enum):
    INDEX = b'index'
    TRAIT = b'trait'


class _Item(NamedTuple):
    key: bytes
    value: bytes
    db: "_DB | None"


class LMDBDataManager(DataManager, HasRequiredTraits):
    """
    LMDBDataManager由一个目录下多个lmdb数据库构成, 其中包含:

    - 一个元数据库(META_ENV)，用于存储元数据, 如主数据库的内存映射大小等
    - 一个主数据库(DATA_ENV)，用于存储数据库, 包含:
        - 一个索引子数据库(_DB.INDEX), 存储数据索引
        - 一个特征子数据库(_DB.TRAIT), 存储数据特征
    """

    path = Directory(exists=True, required=True)

    def _path_changed(self, _):
        self._reset_env()

    def _bind(self, data: Data):
        data_key = data._gid.bytes
        items = [_Item(data_key, dumps(type(data)), _DB.INDEX)]
        for name in data.store_traits:
            value = getattr(data, name)
            items.append(_Item(
                data_key+name.encode(), dumps(value), _DB.TRAIT
            ))
        self._put(*items)

    def _set_data_trait(self, data: Data, name: str, value):
        key = data._gid.bytes + name.encode()
        self._put(_Item(key, dumps(value), _DB.TRAIT))

    def _get_data_trait(self, data: Data, name: str) -> Any:
        key = data._gid.bytes + name.encode()
        with self._begin() as txn:
            buffer = txn.get(key, db=self._dbs[_DB.TRAIT])
        if not buffer:
            raise ValueError("`%s` of %s not in %s" % (name, data, self))
        return loads(buffer)

    """LMDB相关函数"""

    def _reset_env(self):
        if not self._meta_env:
            self._meta_env = lmdb.Environment(
                os.path.join(self.path, META_ENV),
                META_MAP_SIZE, False
            )

        # 使用写事务是为了确保更新DATA_MAP_SIZE与set_mapsize的原子性
        with self._meta_env.begin(write=True) as txn:
            # 从META_DB获取DATA_MAP_SIZE
            map_size = txn.get(DATA_MAP_SIZE, DEFAULT_DATA_MAP_SIZE)
            self._data_map_size = int.from_bytes(map_size)

        del self._env
        self._env = lmdb.Environment(
            os.path.join(self.path, DATA_ENV),
            self._data_map_size, False,
            max_dbs=len(_DB)
        )

        self._dbs = {
            db: self._env.open_db(db.value)
            for db in _DB
        } | {None: None}

    @contextmanager
    def _begin(self, db=None, parent=None, write=False, buffers=False):
        # 本函数用来处理由于其他进程扩容数据库导致的MapResizedError
        # 本函数捕获env.begin()时发生的MapResizedError, 然后在异常处理中重启环境
        # 本函数的关键在于使用上下文管理器封装保持了Transaction的上下文管理器行为
        # 总的来讲, 本函数用于替换env.begin()以尽量避免MapResizedError
        while True:
            try:
                txn = self._env.begin(db=db, parent=parent,
                                      write=write, buffers=buffers)
                break
            except lmdb.MapResizedError:
                logger.debug('Map resized when create new txn, old map_size: %.4f MB',
                             self._env.info()['map_size'] / 1024 ** 2)
                self._reset_env()
                logger.debug('Restart db_env, new map_size: %.4f MB',
                             self._env.info()['map_size'] / 1024 ** 2)
            except lmdb.BadRslotError:
                logger.debug('BadRslotError! Try to restart env!')
                self._reset_env()
        try:
            yield txn
        except:
            txn.abort()
            raise
        txn.commit()

    def _put(self, *items: _Item, txn=None):
        # 本函数主要用来处理由于新增数据超出数据库map_size导致的MapFullError
        # 同时本函数支持同时提交多个键值对(在一个事务内)
        # 本函数捕获MapFullError, 在异常处理时扩容map_size然后重新调用本函数进行提交
        # 本函数在异常处理中的一系列操作意在避免因为不同进程同时设置map_size时导致SIGBUS错误的潜在问题
        # see: https://github.com/jnwatson/py-lmdb/issues/269
        # see: https://bugs.openldap.org/show_bug.cgi?id=9397
        # 但本函数中的处理实现了在不同写txn进程间同步map_size
        try:
            with self._begin(parent=txn, write=True) as _txn:
                for key, value, _db in items:
                    _txn.put(key, value, db=self._dbs[_db])
        except lmdb.MapFullError:
            data_map_size: int = self._env.info()['map_size']
            logger.debug('Map full when put new data, old map_size: %.4f MB',
                         data_map_size / 1024 ** 2)
            with self._meta_env.begin(write=True) as _txn:
                if data_map_size > MAX_DATA_MAP_SIZE_INCREASE:
                    data_map_size += MAX_DATA_MAP_SIZE_INCREASE
                else:
                    data_map_size *= 2
                _txn.put(DATA_MAP_SIZE, data_map_size.to_bytes(
                    DATA_MAP_SIZE_LENGTH))
                self._env.set_mapsize(data_map_size)
            logger.debug('New map_size: %.4f MB',
                         self._env.info()['map_size'] / 1024 ** 2)
            # 再次尝试put
            self._put(*items, txn=txn)