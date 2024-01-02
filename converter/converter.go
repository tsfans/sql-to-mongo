package converter

import "github.com/tsfans/sql_to_mongo/parser"

type SelectConverter interface {
	// 转化为目标查询语句
	Convert() (Query, error)
}

type ConverterValidator interface {
	// 校验SQL是否可转化
	Validate(parser.SQL) error
}

type Query interface {
	// 获取原始SQL
	OriginalSQL() parser.SQL
}
