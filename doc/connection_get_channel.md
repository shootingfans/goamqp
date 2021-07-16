# 单连接获取通道流程介绍

`connection` 的方法 `getChannel` 流程介绍

方法流程图如下
![流程图](https://github.com/shootingfans/goamqp/blob/main/resource/goamqp_connection_get_channel.png?raw=true)

流程说明:

1. 进入 `connection.getChannel` 方法
2. 从链表头节点(`conn.first`)开始遍历
3. 判断当前节点是否在使用
4. 若当前节点空闲则切换当前节点为使用中，空闲数(`conn.idleCount`)减 1，使用数(`conn.usedCount`)加 1，返回节点通道
5. 若当前节点不是最后一个节点(`entry.next != nil`) 则设置当前节点为下一个节点，继续循环，回到步骤 3
6. 若当前节点是最后一个节点，则申请新节点(`conn.allocChannel`)
7. 判断当前是否可申请新节点，当前申请数是否超过了配置中的连接最大通道数配置(`MaximumChannelCountPerConnection`)，则直接返回`ErrChannelMaximum`错误
8. 将申请数(`conn.allocCount`)加 1
9. 开始通过连接申请新通道
10. 申请新通道失败，将申请数(`conn.allocCount`)减 1， 则直接返回错误
11. 申请新通道成功，将空闲数(`conn.idleCount`)加 1， 将新节点放到链表头(新节点`next`指向老链表头节点，老链表头节点`prev`指向新节点，链表头指向新节点)
12. 将当前节点指向链表头，回到步骤 3 继续执行