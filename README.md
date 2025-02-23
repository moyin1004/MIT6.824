# MIT6.824
2021 MIT 6.824 learn

- [x] lab1 mr (2024.02.29)
- [ ] lab2 raft
    - [x] 2A (2024.04.16)
    - [x] 2B (2024.04.16)
    - [x] 2C 持久化 (2024.05.11)
        - 2024.05.05  Figure 8 (unreliable)有概率失败，日志复制超过了2s 需要使用快速回退NextIndex方案
    - [ ] 2D 日志压缩
        - 2025.02.24 apply_msg异步化，applyCh会阻塞造成snapshot时死锁
- [ ] lab3 kvraft
    - [ ] 3A
    - [ ] 3B
- [ ] lab4 shardkv
    - [ ] 4A
    - [ ] 4B