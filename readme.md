# goamqp

## 介绍

goamqp是一个基于 `github.com/streadway/amqp` 的连接池包，其有以下特性

- 支持对多个 `amqp broker` 进行连接
- 支持并发
- 支持阻塞模式
- 支持断开重连
- 伸缩性，对长期不使用的连接和通道进行回收，降低资源占用

## todo

- [X] 基本连接池功能
- [X] 增加对Block的支持 *
- [X] 支持连接重连
- [X] 增加消费的快捷使用
- [X] 增加对队列、交换机、死信队列、延时队列等快捷定义方法
- [X] 支持空闲自动回收

## packages


### consumer

提供了快捷的消费者方法，其实现了 `Looper` 接口，可以在上下文未结束时持续进行消费。

### declare

提供了针对队列定义、交换机定义以及绑定关系的快捷方法。

### publisher

提供了发布的快捷方法

### retry_policy

定义了重试策略，此策略是是针对 `connection` 断开时，如何进行重试操作的定义。

`NewDefaultPolicy` 将创建一个基于 `attemptsMaximum (最大重试次数)` 以及 `interval (重试间隔)` 的重试操作，一旦超过最大重试次数，则会调用 `final` 方法，并传入重试连接错误
