# law-router

独立的中转 + 聊天项目。和 PAAS_Server 无关。

提供：
- **NewAPI** — 公开的 LLM API 中转服务器（`/v1/*`）
- **Kong Gateway** — 公共入口，路由 + 记录所有聊天流量
- **Postgres** — 独立数据库（NewAPI + chat_log 日志）
- (可选) **LibreChat** — 聊天 UI

所有聊天内容（用户输入 + AI 生成）都会通过 Kong 的 http-log 插件记录到 `chat_log` 数据库。

## 项目结构

```
law-router/
├── helm/                 # Helm chart（部署清单）
│   ├── Chart.yaml
│   ├── values.yaml
│   ├── files/            # log-collector 源码
│   └── templates/        # k8s 资源模板
├── docs/                 # 设计文档、架构说明
├── scripts/              # 运维脚本（部署、测试辅助）
├── tests/                # e2e / 集成测试
└── .mcp.json             # SSH MCP 配置（远程 k3s）
```

## 部署

通过 ArgoCD 管理（chart 仓库：`law-ai-official/helm-ai-gateway`）：

```bash
kubectl apply -f apps/helm-ai-gateway.yaml
```

或本地直接部署：

```bash
helm install ai-gateway ./helm -n ai-gateway --create-namespace
```

## 访问

- Kong Gateway（公开入口）：`http://23.144.68.246:30080`
- NewAPI 管理后台：`http://23.144.68.246:31800`
- 聊天 API：`http://23.144.68.246:30080/v1/chat/completions`
