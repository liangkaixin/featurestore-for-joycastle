## 整体结构
```plaintext
featurestore-for-joycastle/
├── .idea/                  # IDE配置文件目录
├── docs/                   # 文档目录
│   ├── MJ Consumer.md      # 消费者文档
│   ├── consumer流程图.png  # 消费者流程图
│   ├── MJ Dashboard.md     # 仪表板文档
│   ├── img.png             # 图片资源
│   └── 指标大盘.pdf        # 指标分析报告
├── utils/                  # 核心代码目录
│   ├── Consumer.py         # Kafka消费者实现
│   ├── game_events.db      # SQLite数据库文件
│   ├── Producer.py         # Kafka生产者实现
│   ├── MJ README.md        # 项目说明文档
│   ├── requirements.txt    # Python依赖文件
│   └── run.sh              # 项目运行脚本
```
![代码结构.png](docs/%E4%BB%A3%E7%A0%81%E7%BB%93%E6%9E%84.png)

## 运行方式
### docker部署Kafka + 运行程序 + 部署 Metabase

```shell
./run.py
```

### 单独运行程序
```sh
    python main.py
```