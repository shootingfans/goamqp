# goamqp

## 介绍

goamqp是一个基于 `github.com/streadway/amqp` 的连接池包，其有以下特性

- 支持对多个 `amqp broker` 进行连接
- 支持并发
- 伸缩性，对长期不使用的连接和通道进行回收，降低资源占用

## todo

[ ] 基本连接池功能
[ ] 支持空闲自动回收