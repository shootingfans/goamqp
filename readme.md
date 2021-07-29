# goamqp

## 介绍

goamqp是一个基于 `github.com/streadway/amqp` 的连接池包，其有以下特性

- 支持对多个 `amqp broker` 进行连接
- 支持并发
- 支持阻塞模式
- 支持断开重连
- 伸缩性，对长期不使用的连接和通道进行回收，降低资源占用

## todo

- [x] 基本连接池功能
- [x] 增加对Block的支持
- [x] 支持连接重连
- [x] 增加消费的快捷使用
- [ ] 增加对队列、交换机、死信队列、延时队列等快捷定义方法
- [ ] 支持空闲自动回收
