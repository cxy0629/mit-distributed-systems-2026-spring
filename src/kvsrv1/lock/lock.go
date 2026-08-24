package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk // KV客户端句柄
	lockname string          // 分布式锁的全局名称，不同client使用相同lockname竞争同一把锁
	id       string          // Lock实例的身份信息，Acquire成功后写入KV server，记录谁持有锁
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		id:       kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// 获取KV server内的锁信息
		value, version, err := lk.ck.Get(lk.lockname)

		if err == rpc.ErrNoKey {
			// 锁信息不存在，则创建锁（创建也是一种锁竞争）
			err := lk.ck.Put(lk.lockname, lk.id, 0)

			// 竞争成功
			if err == rpc.OK {
				return
			}

			// client端ErrMaybe说明server端发生了ErrVersion且client发生了重试
			// 可能Put锁竞争成功并成功写入KV server但成功回复丢失，重试后Version已经被本client更新引发server端ErrVersion
			// 可能Put未生效，重试后Version已经被其他client更新引发server端ErrVersion
			if err == rpc.ErrMaybe {
				// 再次获取KV server内锁信息，确认是否持有锁
				v, _, e := lk.ck.Get(lk.lockname)
				if v == lk.id && e == rpc.OK {
					return
				}
			}
		}

		if err == rpc.OK {
			// 锁存在，可能空闲，可能被本client持有，可能被其他client持有

			if value == lk.id {
				// 被本client持有
				return
			}
			if value == "" {
				// 空闲
				err := lk.ck.Put(lk.lockname, lk.id, version)
				if err == rpc.OK {
					return
				}
				if err == rpc.ErrMaybe {
					v, _, e := lk.ck.Get(lk.lockname)
					if v == lk.id && e == rpc.OK {
						return
					}
				}
			}

			//被其他client持有，什么都不做
		}

		// 沉睡，等待下一次竞争
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.lockname)

		if err != rpc.OK {
			return
		}

		if value != lk.id {
			return
		}

		err = lk.ck.Put(lk.lockname, "", version)

		if err == rpc.OK {
			return
		}

		if err == rpc.ErrMaybe {
			v, _, e := lk.ck.Get(lk.lockname)
			// 注意此时锁已经可能被其他client持有，v不一定为""
			if v != lk.id && e == rpc.OK {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}
