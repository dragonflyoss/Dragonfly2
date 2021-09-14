# 测试

Dragonfly 包括单元测试和 E2E 测试。

## 单元测试

单元测试代码项目目录中。

### 运行单元测试

```bash
$ make test
```

### 运行单元测试并产出测试报告

```bash
$ make test-coverage
```

## E2E 测试

E2E 测试代码在 `test/e2e` 目录中。

### 运行 E2E 测试

```bash
$ make e2e-test
```

### 运行 E2E 测试并产出测试报告

```bash
$ make e2e-test-coverage
```

### 清理 E2E 测试

```bash
$ make clean-e2e-test
```
