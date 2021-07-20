# 连接池获取通道流程介绍

`Pool` 接口定义的 `GetChannel` 流程介绍

## 流程

![流程图](https://github.com/shootingfans/goamqp/blob/main/resource/pool_get_channel.png?raw=true)

## 流程说明

1. 进入 `Pool` 接口定义的 `GetChannel` 方法
2. 判断当前是否连接池关闭，关闭则直接返回 `ErrPoolClosed` 错误
3. 从链表头节点(`pool.first`)开始遍历
4. 判断当前节点是否繁忙(`空闲数量 > 0 || 单连接通道限制 = 0 || 当前申请通道数 < 单连接通道限制`)
5. 若不繁忙，则调用 `connection.getChannel` 从连接获取通道
6. 成功获取通道则直接返回通道
7. 获取通道或者节点繁忙，则获取下一个节点
8. 若下一个节点非nil，则返回4继续遍历
9. 申请新的连接
10. 如果达到最大连接数限制，返回 `ErrChannelMaximum`
11. 轮询获取下一个节点地址
12. 连接申请数(`pool.allocCount`)加 1
13. 通过节点地址创建连接
14. 若创建连接失败，则申请数(`pool.allocCount`)减 1，直接返回错误
15. 创建连接成功，将新节点放到链表头(新节点`next`指向老链表头节点，老链表头节点`prev`指向新节点，链表头指向新节点)
16. 将当前节点指向链表头，回到步骤 4 继续执行