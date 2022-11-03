导入代码后的一些问题

1. 将代码导入到编辑工具之后，如果没有运行一次，会发现很多类不存在的问题，这些类都是对应模块下 /main/resources/common/message 下的 json 文件，运行的时候会自动将这些 json 生成对应的 class 文件
2. 运行的时候如果发现报错退出了，可以尝试看是否是这个方法org.apache.kafka.common.utils.Utils#atomicMoveWithFallback(java.nio.file.Path, java.nio.file.Path, boolean)报错
    该方法是做文件替换，这里可能是操作系统导致的问题，如果报错可以将该方法里面的代码替换成
```java
source.toFile().renameTo(target.toFile());
```