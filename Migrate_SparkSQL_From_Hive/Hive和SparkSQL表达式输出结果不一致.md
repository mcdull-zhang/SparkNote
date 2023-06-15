Hive版本是0.13.0，Spark版本是3.2.0，该文章所列的差异均是在这两个版本中测试的，有时候也会对照Hive2.3.7的表现。

### 1.Union的隐式类型转换

**scala/java中的类型转换**

首先在java/scala中，这样的类型转换是会报错的：

> Double类型 -> String类型 -> Int类型/Long类型

比如：

```scala
scala> "35410343".toDouble.toString.toLong
java.lang.NumberFormatException: For input string: "3.5410343E7"
  at java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
  at java.lang.Long.parseLong(Long.java:589)
  at java.lang.Long.parseLong(Long.java:631)
  at scala.collection.immutable.StringLike$class.toLong(StringLike.scala:277)
  at scala.collection.immutable.StringOps.toLong(StringOps.scala:29)
  ... 32 elided
```

**第一个案例**

了解了java/scala类型转换的问题，我们直接看下面这条SQL：

```sql
SELECT cast(col1 as bigint) FROM 
(
  SELECT sum('35410343') AS col1 
  UNION ALL 
  SELECT '35410343' AS col1
) a
```

在Spark、Hive0.13、Hive2.3.7中该SQL的执行结果都是

```tex
NULL
35410343
```

输出结果是符合预期的，因为`Union`的左右`plan`分别返回的类型是`double`和`string`，`Union`会对左右`plan`的`output`进行类型上的统一，`string`的优先级是要比`double`要高的，所以都会转成`string`。

可以认为`Union`将`select sum('35410343') as col1`转为了`select cast(sum('35410343') as string) as col1`。

然后`Union`之上还有一个`Project`计算，所以最终的执行逻辑类似下面这条SQL：

```sql
SELECT cast(cast(sum('35410343') AS string) AS bigint) AS col1 
UNION ALL 
SELECT cast('35410343' as bigint) AS col1
```

**奇怪的第二个案例**

```sql
SELECT cast(col1 as bigint) FROM 
(
  SELECT '35410343' AS col1 
  UNION ALL
  SELECT sum('35410343') AS col1
) a
```

这个SQL可以说跟第一个SQL很相似，仅仅把`union`的左右`plan`调换了一下。

Spark和Hive2.3.7的返回结果同第一个案例。

但是hive0.13却返回：

```tex
35410343
35410343
```

具体原因和Hive0.13是如何确定`union`的输出类型规则有关，更具体的细节我会在另外的文章描述。



### 2.Transform结果不一致

#### 2.1当脚本返回的字段个数大于接收字段个数

```sql
SELECT TRANSFORM('a','b','c') USING 'cat' AS (c1 string, c2 string)
```

在Spark中返回的c1列的值是`a`，c2列的值为`bc`

在Hive中返回的c1列的值是`a`，c2列的值为`b`

Spark的最后一列会接收剩余所有数据，而Hive则是强制按字段依次接收。



#### 2.2处理struct类型的区别

```sql
SELECT TRANSFORM(struct('tom','mary','tim')) USING 'cat' AS (col string)
```

Spark输出

```tex
tommarytim
```

Hive输出

```tex
{"col1":"tom","col2":"mary","col3":"tim"}
```

可以看到Hive是将`struct`转为`json`后发给脚本进行处理的。



### 3.like不兼容

```shell
spark-sql> select 'x\\xx' like '%\\x%';
the pattern '%\x%' is invalid, the escape character is not allowed to precede 'x'

hive> select 'x\\xx' like '%\\x%';
true
```

在Spark中like函数需要借助于java的正则表达式来执行，所以需要把like参数中的pattern转为java中的正则表达式。这个转换过程也很简单，就是将pattern中的`_`转成`.`，把pattern中的`%`转成`.*`。下面是具体的代码：

```scala
in.next match {
  case c1 if c1 == escapeChar && in.hasNext =>
	  val c = in.next
    c match {
  	  // 如果是转义字符后面跟着_或者%,则也不需要转
      case '_' | '%' => out ++= Pattern.quote(Character.toString(c))
      // 如果转移字符后面跟着转义字符，也就是\\这样的形式，则保存一个\
      case c if c == escapeChar => out ++= Pattern.quote(Character.toString(c))
      // 转义字符后面跟着_或%或\这三个字符之外的其他字符，直接报错
      case _ => fail(s"the escape character is not allowed to precede '$c'")
    }
  // 将 _ 转成 .
  case '_' => out ++= "."
  // 将 % 转成 .*
  case '%' => out ++= ".*"
  // 其他字符不需要转换
  case c => out ++= Pattern.quote(Character.toString(c))
}
```

可以看到转义字符后面只允许跟着`_`或者`%`或者`\`，这三种字符，其他情况均报错。



### 4.reflect、java_method不兼容

当调用一些静态函数抛出异常时，Hive仅仅会打印下日志，当前行会返回NULL值。

```sql
hive> select reflect('java.net.URLDecoder','decode',name,'UTF-8') from t;
OK
UDFReflect evaluate java.lang.reflect.InvocationTargetException method = public static java.lang.String java.net.URLDecoder.decode(java.lang.String,java.lang.String) throws java.io.UnsupportedEncodingException args = [null, UTF-8]
3
NULL
2
```

Spark中该SQL会执行失败。



### 5.unix_timestamp不兼容

```shell
hive> select unix_timestamp('0000-00-00 00:00:00', 'yyyy-MM-dd HH:mm:ss');
-62170185600

spark-sql> select unix_timestamp('0000-00-00 00:00:00', 'yyyy-MM-dd HH:mm:ss')
NULL
```



### 6.string和int比较时类型转换规则不同

```sql
create table t1 (id string);
insert into table t1 values ('1'),('2'),('003'),('010f');
```

在Spark中`string`和`int`做比较的时候，会转成`int`来做比较。

在Hive中`string`和`int`做比较的时候，会转成`double`来做比较。

```shell
spark-sql> select * from t1 where id>1;
2
003

hive> select * from t1 where id>1;
2
003
010f
```



### 7.parse_url不兼容

```shell
spark-sql> select parse_url('http://facebook.com/path/p1.php?query=1&newid=jajaext{} ', 'QUERY','newid');
NULL

hive> select parse_url('http://facebook.com/path/p1.php?query=1&newid=jajaext{} ', 'QUERY','newid');
jajaext{}
```

原因是Hive将字符串转为URL， SparkSQL转为URI，而URI 在初始化解析时发现特殊字符，会返回 NULL



### 8.pmod函数不兼容

```sql
hive> select pmod(30, -7);
-5

spark-sql> select pmod(30, -7);
2
```



### 9.grouping set

```sql
select 
  id, count(1) 
from 
  dongdong_person7 
group by 
  id,name 
grouping sets ((id), (id))
```

执行结果：

```tex
# Hive结果
5		2

# Spark结果
5		1
5		1
```

Spark会执行两遍`group by id`,类似于`group by id union all group by id`

但是会将两遍的`group by id`的结果再merge一次。



### 10.grouping__id的逻辑更改

Grouping__ID规则在hive2.3.0中被更改过,更改后更符合SQL标准和其他的SQL引擎，spark的规则跟hive2.3版本保持一致。

详细内容：https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup



### 11.get_json_object

```shell
hive> select get_json_object('{\"key\":[{\"inner_key\":\"1\"},{\"inner_key\":\"2\"}]}', '$.key.inner_key');
["1","2"]

hive> select get_json_object('{\"key\":[{\"inner_key\":\"1\"},{\"inner_key\":\"2\"}]}', '$.key[*].inner_key');
["1","2"]

spark-sql> select get_json_object('{\"key\":[{\"inner_key\":\"1\"},{\"inner_key\":\"2\"}]}', '$.key.inner_key');
NULL

spark-sql> select get_json_object('{\"key\":[{\"inner_key\":\"1\"},{\"inner_key\":\"2\"}]}', '$.key[*].inner_key');
["1","2"]
```



### 12.产生的文件名不同

Hive输出文件名

```tex
000000_0
000001_0
000002_0
```

Spark文件名

```tex
part-00000-0360a944-b115-4449-84d7-ade53172a75b.c000.snappy.orc
part-00001-0360a944-b115-4449-84d7-ade53172a75b.c000.snappy.orc
part-00002-0360a944-b115-4449-84d7-ade53172a75b.c000.snappy.orc
```



### 13.hash函数不兼容

```shell
hive> select hash('a');
97

spark-sql> select hash('a');
1485273170
```



### 14.to_date对特殊值的处理

```shell
hive> select to_date('0000-00-00 00:00:00');
0002-11-30

spark-sql> select to_date('0000-00-00 00:00:00');
NULL
```



### 15.cast

```shell
hive> select cast('1\t' as bigint);
NULL

spark-sql> select cast('1\t' as bigint)
1

spark-sql> select cast(' 1\t' as bigint)
1
```

Spark在cast会做trim操作，也就是把输入的空格制表符都去掉。



### 16.stddev

区别一：

hive和spark都有三个标准偏差函数：

1. stddev_pop()：总体标准方差
2. stddev_samp()：样本标准方差
3. stddev()：在Spark中等价于stddev_samp()，在hive中等价于stddev_pop()

区别二：

当输入数据只有一行时，hive和spark计算结果不一致：

```sql
select 
	col, stddev_pop(num),stddev_samp(num),stddev(num) as stddev_col
from (
  select 'A' as col, '1' as num
  union all
  select 'B' as col, '2' as num
) as a
group by col
```

Spark的执行结果：

```tex
A 0.0 NULL NULL
B 0.0 NULL NULL
```

Hive的执行结果

```tex
A 0.0 0.0 0.0
B 0.0 0.0 0.0
```



