# 目标文件名
BINARY = coredhcp

# 源代码目录
SRC_DIR = .

# 默认目标：编译并运行
.PHONY: all
all: build run

# 编译目标
.PHONY: build
build:
	@echo "Building $(BINARY)..."
	@go build -o $(BINARY) $(SRC_DIR)/main.go
	@echo "Build complete!"

# 编译并显示详细信息
.PHONY: x
x:
	@echo "Building $(BINARY) with detailed output (-x)..."
	@go build -x -o $(BINARY) $(SRC_DIR)/main.go
	@echo "Build complete!"
	@echo "Running $(BINARY) with sudo... (Press Ctrl+C to stop)"
	@trap "echo '\nGracefully stopping $(BINARY)...'; exit 0" INT
	@sudo ./$(BINARY)

# 运行目标
.PHONY: run
run:
	@echo "Running $(BINARY) with sudo... (Press Ctrl+C to stop)"
	@trap "echo '\nGracefully stopping $(BINARY)...'; exit 0" INT
	@sudo ./$(BINARY)

# 清理目标
.PHONY: clean
clean:
	@echo "Cleaning up..."
	@rm -f $(BINARY)
	@echo "Clean complete!"

# 帮助信息
.PHONY: help
help:
	@echo "Makefile targets:"
	@echo "  make            - Build and run the project"
	@echo "  make build      - Compile the project"
	@echo "  make run        - Run the binary"
	@echo "  make clean      - Remove compiled binary"
	@echo "  make x          - Build with 'go build -x' and run the binary"
	@echo "  make help       - Display this help message"

