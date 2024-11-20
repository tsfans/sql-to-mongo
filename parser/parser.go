package parser

// SQL解析器，包含：语法树解析、语法校验、优化改写
type SelectParser interface {
	// 返回SQL元数据
	Parse(strictMode bool) (SQL, error)
}

// 代表一个SQL对象
type SQL interface {
	// 获取原始SQL
	OriginalSQL() string
}
