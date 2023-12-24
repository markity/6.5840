### 6.5840 LAB

当前状态

- 2A: OK, 1w测试通过
- 2B: OK, 1w测试通过, 但是注意进程别开太多了, 可能由于gc延时触发figure8提交限制而失败, 我个人测试100进程同时跑1w次会出现一次
- 2C: OK, 1w测试通过
- 2D: OK, 1w测试通过
- 3A: OK, 还没测太多, 这玩意太卡了
- 3B: IN PROGRESS

### Test

Test 50 times and use 8 workers:

```bash
> cd raft
> python3 ./dtest.py -n 50 -p 8 2A 2B 2C 2D 3A
```

### Ref

https://zhuanlan.zhihu.com/p/672530996