package goamqp

import "unsafe"

// entry 用于做链表结构使用
type entry struct {
	payload interface{}    // 载体
	next    unsafe.Pointer // 指向下一个，当为最后一个时，此为nil
	prev    unsafe.Pointer // 指向上一个，当为第一个时，此为nil
}
