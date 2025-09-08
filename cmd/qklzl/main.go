package main

import (
	"log"

	"qklzl/internal/app"
	"qklzl/internal/config"
)

func main() {
	// 解析配置
	cfg, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("解析配置失败: %v", err)
	}

	// 创建应用程序实例
	app, err := app.New(cfg)
	if err != nil {
		log.Fatalf("创建应用程序失败: %v", err)
	}

	// 初始化应用程序
	if err := app.Initialize(); err != nil {
		log.Fatalf("初始化应用程序失败: %v", err)
	}

	// 运行应用程序
	if err := app.Run(); err != nil {
		log.Fatalf("运行应用程序失败: %v", err)
	}
}