# -*- coding:utf-8 -*- 
#Author: Henry

import asyncio, logging
import aiomysql
logging.basicConfig(level=logging.INFO)

# 打印SQL查询语句
def log(sql, args=()):
    logging.info('SQL: %s' %(sql))

# 创建一个全局的连接池，每个HTTP请求都从池中获得数据库连接
# @asyncio.coroutine 将一个生成器 generator 标记为 coroutine 类型，然后扔进 Eventloop 异步执行
@asyncio.coroutine
def create_pool(loop, **kw):  # 这里的 **kw 是一个 dict
    logging.info('create database connection pool...')

    # 全局变量 __pool 用于存储整个连接池
    global __pool
    # 理解这里的 yield from 是很重要的
    # 返回一个 pool 实例
    __pool = yield from aiomysql.create_pool(
        # **kw 参数可以包含所有连接需要用到的关键字参数
        # dict 有一个 get 方法，如果dict中有对应的value值，则返回对应于key的value值，否则返回默认值，
        # 例如下面的host，如果dict里面没有 'host',则返回后面的默认值，也就是'localhost' 即本机IP
        host = kw.get('host', 'localhost'),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        port = kw.get('poot', 3306),
        charset = kw.get('charset', 'utf-8'),
        # 自动提交事务，不用手动提交事务
        autocommit = kw.get('autocommit', True),
        # 默认最大连接数为 10，最小连接数为 1
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        #接收一个event_loop实例
        loop = loop
    )

@asyncio.coroutine
def destroy_pool():
    global __pool
    if __pool is not None :
        __pool.close()  # 关闭进程池,The method is not a coroutine,就是说close()不是一个协程，所有不用yield from
        yield from __pool.wait_closed()  # 但是 wait_close() 是一个协程，所以要用yield from,到底哪些函数是协程，上面Pool的链接中都有

# 封装SQL SELECT语句为select函数
@asyncio.coroutine
def select(sql, args, size = None):
    log(sql, args)
    global __pool
    # -*- yield from 将会调用一个子协程，并直接返回调用的结果
    # yield from 从连接池中返回一个连接,这个地方已经创建了进程池并和进程池连接了,进程池的创建被封装到了create_pool(loop, **kw)
    with (yield from __pool) as conn:
        # DictCursor is a cursor which returns results as a dictionary
        cursor = yield from conn.cursor(aiomysql.DictCursor)

        # 执行SQL语句
        # SQL语句的占位符为 ？，MySQL的占位符为 %s
        yield from cursor.execute(sql.replace('?', '%s'), args or ())

        # 根据指定返回的 size，返回查询的结果
        if size:
            # 返回 size 条查询结果,结果是一个 list,里面是 tuple
            rs = yield from fetchmany(size)
        else:
            # 返回所有查询结果
            rs = yield from fetchall()

        # 关闭游标，不用手动关闭conn，因为是在with语句里面，会自动关闭，因为是select，所以不需要提交事务 commit
        yield from cursor.close()
        logging.info('rows return: %s' %(len(rs)))
    return rs  # 返回查询结果，元素是 tuple 的 list

# 封装INSERT, UPDATE, DELETE语句
# 这三者的语句操作参数一样，所以定义一个通用的执行函数,只是操作参数一样,但是语句的格式不一样
# 返回操作影响的行号
@asyncio.coroutine
def execute(sql, args):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        try:
            # execute类型的SQL操作返回的结果只有行号，所以不需要用DictCursor
            cursor = yield from conn.cursor()
            yield from cursor.execute(sql.replace('?', '%s'), args)
            yield from conn.commit()  # 这里为什么还要手动提交数据,试一下有没有这个必要？？
            affectedLine = cursor.rowcount
            yield from cursor.close()
        except BaseException as e:
            raise
        return affectedLine

# 这个函数主要是把查询字段计数 替换成sql识别的?
# 比如说：insert into  `User` (`password`, `email`, `name`, `id`) values (?,?,?,?)  看到了么 后面这四个问号
# 根据输入的参数生成占位符列表
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')

    # 以','为分隔符，将列表合成字符串
    return (','.join(L))

# 定义Field类，负责保存(数据库)表的字段名和字段类型
# 将数据库的类型与 Python 进行对应
class Field(object):
    # 表的字段包含名字、类型、是否为表的主键和默认值
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    # 当打印(数据库)表时，输出(数据库)表的信息：类名，字段类型和名字
    def __str__(self):
        # 返回 类名 字段类型 和 名字
        return "<%s , %s , %s>" % (self.__class__.__name__, self.column_type, self.name)
        # return ('<%s, %s: %s>' %(self.__class__.__name__, self.column_type, self.name))

# -*- 定义不同类型的衍生Field -*-
# 定义数据库中五个存储类型
# -*- 表的不同列的字段的类型不一样
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, column_type='varchar(100)'):
        super().__init__(name, column_type, primary_key, default)

# 布尔类型不可以做主键
class BooleanField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'Text', False, default)

# -*-定义Model的元类

# 所有的元类都继承自type
# ModelMetaclass元类定义了所有Model基类(继承ModelMetaclass)的子类实现的操作

# -*-ModelMetaclass的工作主要是为一个数据库表映射成一个封装的类做准备：
# ***读取具体子类(user)的映射信息
# 创造类的时候，排除对Model类的修改
# 在当前类中查找所有的类属性(attrs)，如果找到Field属性，就将其保存到__mappings__的dict中，同时从类属性中删除Field(防止实例属性遮住类的同名属性)
# 将数据库表名保存到__table__中

# 完成这些工作就可以在Model中定义各种数据库的操作方法

class ModelMetaclass(type):
    # __new__控制__init__的执行，所以在其执行之前
    # cls:代表要__init__的类，此参数在实例化时由Python解释器自动提供(例如下文的User和Model)
    # bases：代表继承父类的集合
    # attrs：类的方法集合
    def __new__(cls, name, bases, attrs):

        # 排除对 Model 类的修改
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        # 获取 table 名字
        tableName = attrs.get('__table__', None) or name  # 如果存在表名，则返回表名，否则返回 name 
        logging.info('found model: %s (table: %s)' % (name, tableName))

        # 获取 Field 所有主键名和 Field
        mappings = dict()
        fields = []  # fields保存的是除主键外的属性名
        primaryKey = None
        # k 表示类的一个属性
        for k, v in attrs.items():
            # Field 属性
            if isinstance(v, Field):
                # 此处打印的k是类的一个属性，v是这个属性在数据库中对应的Field列表属性
                logging.info('found mapping: %s --> %s' % (k, v))
                mappings[k] = v

                # 找到了主键
                if v.primary_key:
                    logging.info('fond primary key %s' % k)

                    # 如果此时类实例已存在主键，说明主键重复了
                    if primaryKey:
                        # 一个表只能有一个主键，当再出现一个主键的时候就报错
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    # 否则将此列设为列表的主键
                    primaryKey = k
                else:
                    fields.append(k)
        # end for

        if not primaryKey:
            # 如果主键不存在也将会报错，在这个表中没有找到主键，一个表只能有一个主键，而且必须有一个主键
            raise StandardError('Primary key is nor founnd')

        # 从类属性中删除Field属性
        for k in mappings.keys():
            attrs.pop(k)

        # 保存除主键外的属性名为``（运算出字符串）列表形式
        # 将除主键外的其他属性变成`id`, `name`这种形式，关于反引号``的用法，可以参考点击打开链接
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))

        # 保存属性和列的映射关系
        attrs['__mappings__'] = mappings
        # 保存表名
        attrs['__table__'] = tableName
        # 保存主键属性名
        attrs['__primary_key__'] = primaryKey
        # 保存除主键外的属性名
        attrs['__fields__'] = fields

        # 构造默认的SELECT、INSERT、UPDATE、DELETE语句
        # ``反引号功能同repr()
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into  `%s` (%s, `%s`) values(%s)' % (
        tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set `%s` where `%s` = ?' % (
        tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from  `%s` where `%s`=?' % (tableName, primaryKey)

        return type.__new__(cls, name, bases, attrs)

# 定义ORM所有映射的基类：Model
# Model类的任意子类可以映射一个数据库表
# Model类可以看作是对所有数据库表操作的基本定义的映射

# 基于字典查询形式
# Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__，能够实现属性操作
# 实现数据库操作的所有方法，定义为class方法，所有继承自Model的都具有数据库操作方法

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):  # 感觉这里去掉__init__的声明对代码结果也是没影响的
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r'"Model" object has no attribute：%s' % (key))

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        # 内置函数getattr会自动处理
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if not value:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    # 类方法有类变量cls传入，从而可以用cls做一些相关的处理。并且有子类继承时，调用该类方法时，传入的类变量cls是子类，而非父类。
    @asyncio.coroutine
    def findAll(cls, where=None, args=None, **kw):
        '''find objects by where clause'''
        sql = [cls.__select__]

        if where:
            sql.append('where')
            sql.append(where)

        if args is None:
            args = []

        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)

        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?,?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = yield from select(' '.join(sql), args)  # 返回的rs是一个元素是 tuple 的 list
        return [cls(**r) for r in rs]  # **r 是关键字参数，构成了一个cls类的列表，其实就是每一条记录对应的类实例

    @classmethod
    @asyncio.coroutine
    def findNumber(cls, selectField, where=None, args=None):
        '''find number by select and where.'''
        sql = ['select %s __num__ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = yield from select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['__num__']

    @classmethod
    @asyncio.coroutine
    def find(cls, primarykey):
        '''find object by primary key'''
        # rs是一个list，里面是一个dict
        rs = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [primarykey], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])  # 返回一条记录，以 dict 的形式返回，因为 cls 的父类继承了dict类

    # 实例方法
    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    @asyncio.coroutine
    def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__updata__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    @asyncio.coroutine
    def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = yield from execute(self.__updata__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)