# mvn打包指令

mvn package -Dmaven.compiler.target=1.8 -Dmaven.compiler.source=1.8

# conf/spark-defaults.conf配置

```
spark.eventLog.enabled           true
spark.eventLog.dir               file:///d:/Spark/spark-2.1.3-bin-hadoop2.6/event-logs
spark.history.fs.logDirectory     file:///d:/Spark/spark-2.1.3-bin-hadoop2.6/event-logs
spark.sql.queryExecutionListeners com.huawei.cloudviews.spark.listeners.PlanLogListener
```

# 开启HistoryServer指令

spark-class.cmd org.apache.spark.deploy.history.HistoryServer

# 其他操作

cloudviews-core的target下面生成的包，拷贝到spark的jars目录下，然后再D:\Spark\spark-2.1.3-bin-hadoop2.6\conf\spark-defaults.conf里面加上

spark.sql.queryExecutionListeners com.huawei.cloudviews.spark.listeners.PlanLogListener，相当于加了个监听器。

# 疑问

1. 目前计算哈希的顺序是从顶往下计算，总共计算了哪些信息的哈希值呢？

2. LogicPlanSignature.java里面getLocalSignature方法中包括的一些属性：LogicalRelation、stringArgs这些表示的是什么意思？

3. 按照3月14日的讨论，只考虑Union、Join这些操作的哈希，那么对于中间具有其他节点的怎么处理？直接将这些节点的哈希值表示为null。

4. getLocalSignature表示的应该是获取当前节点中有效信息的哈希的意思。

5. 对于顺序问题不考虑的情况，方案可以为：将自顶向下计算改成自底向上计算，计算的每个孩子的哈希不直接进行合并，而是先保存再排序，最后合并，再合并上父节点的哈希。

# 获取csv文件

1. java -Xmx8g -jar cloudviews-spark-0.4-SNAPSHOT.jar SparkLogicalWorkloadParserTask cloudviews-spark.properties

2. F:\Physical View\cloudviews-src\cloudviews-spark\apps\sparkcruise\bash\cloudviews-spark.properties是依赖的配置文件

3. logical_ir.csv是生成的文件

4. com.huawei.cloudviews.spark.planir.parsers.SparkQueryParser#parsePlan

5. com.huawei.cloudviews.spark.listeners.PlanLogListener#getJsonAnnotations打印到日志里面的东西，需要修改

# 2022.3.16问题记录

1. com.huawei.cloudviews.core.signatures.hash.SignHash64#Compute(java.lang.String, java.lang.String)中的逻辑可不可以修改一下？

2. com.huawei.cloudviews.core.planir.preprocess.enumerators.ViewEnumerator#setOperatorInfo，设置一些列的值

3. com.huawei.cloudviews.spark.planir.parsers.ApplicationLogParser#parseWorkloadLine，遍历日志里面的每一行

4. com.huawei.cloudviews.core.planir.preprocess.entities.View#getHeader设置csv文件列名

5. com.huawei.cloudviews.spark.planir.parsers.SparkQueryParser#getAnnotations获取打印的信息


