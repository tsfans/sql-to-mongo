package parser

import (
	"fmt"
	"slices"
	"strings"

	tiParser "github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/types"
	parserDriver "github.com/pingcap/tidb/types/parser_driver"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
)

type MySQLSelectParser struct {
	sql         string
	parser      *tiParser.Parser
	ast         *ast.SelectStmt
	sqlSelect   *SQLSelect
	tableSchema map[string][]*Column
}

type SQLSelect struct {
	Distinct bool
	Fields   []ExprField
	From     *SQLTable
	Where    ExprField
	GroupBy  []ExprField
	Having   ExprField
	OrderBy  []*SQLOrderBy
	Limit    *SQLLimit
	SQL      *string
}

func (s SQLSelect) OriginalSQL() string {
	if s.SQL != nil {
		return *s.SQL
	}
	return ""
}

type SQLFieldType int

const (
	// 普通字段
	SQLField_Type_Normal SQLFieldType = 1
	// 取值函数
	SQLField_Type_Func SQLFieldType = 2
	// 聚合函数
	SQLField_Type_Agg_Func SQLFieldType = 3
	// 通配符*
	SQLField_Type_WildCard SQLFieldType = 4
	// 显式值
	SQLField_Type_Value SQLFieldType = 5

	SQLField_Type_WildCard_Value string = "*"

	SQLFuncName_Sum          string = "SUM"
	SQLFuncName_Avg          string = "AVG"
	SQLFuncName_Count        string = "COUNT"
	SQLFuncName_Max          string = "MAX"
	SQLFuncName_Min          string = "MIN"
	SQLFuncName_Json_Arr_Agg string = "JSON_ARRAYAGG"
	SQLFuncName_Date         string = "DATE"

	SQLFuncName_Json_Extract    string = "JSON_EXTRACT"
	SQLFuncName_Year            string = "YEAR"
	SQLFuncName_Month           string = "MONTH"
	SQLFuncName_Day             string = "DAY"
	SQLFuncName_Hour            string = "HOUR"
	SQLFuncName_Minute          string = "MINUTE"
	SQLFuncName_Second          string = "SECOND"
	SQLFuncName_Date_Format     string = "DATE_FORMAT"
	SQLFuncName_Round           string = "ROUND"
	SQLFuncName_If              string = "IF"
	SQLFuncName_Case            string = "CASE"
	SQLFuncName_Now             string = "NOW"
	SQLFuncName_To_Double       string = "TO_DOUBLE"
	SQLFuncName_To_Floor        string = "FLOOR"
	SQLFuncName_Substring_Index string = "SUBSTRING_INDEX"
)

var (
	SQLFunc_Agg_Map = map[string]string{
		SQLFuncName_Sum:          SQLFuncName_Sum,
		SQLFuncName_Avg:          SQLFuncName_Avg,
		SQLFuncName_Count:        SQLFuncName_Count,
		SQLFuncName_Max:          SQLFuncName_Max,
		SQLFuncName_Min:          SQLFuncName_Min,
		SQLFuncName_Json_Arr_Agg: SQLFuncName_Json_Arr_Agg,
	}
	SQLFunc_Map = map[string]string{
		SQLFuncName_Json_Extract:    SQLFuncName_Json_Extract,
		SQLFuncName_Year:            SQLFuncName_Year,
		SQLFuncName_Month:           SQLFuncName_Month,
		SQLFuncName_Day:             SQLFuncName_Day,
		SQLFuncName_Hour:            SQLFuncName_Hour,
		SQLFuncName_Minute:          SQLFuncName_Minute,
		SQLFuncName_Second:          SQLFuncName_Second,
		SQLFuncName_Date_Format:     SQLFuncName_Date_Format,
		SQLFuncName_Round:           SQLFuncName_Round,
		SQLFuncName_If:              SQLFuncName_If,
		SQLFuncName_Now:             SQLFuncName_Now,
		SQLFuncName_To_Double:       SQLFuncName_To_Double,
		SQLFuncName_To_Floor:        SQLFuncName_To_Floor,
		SQLFuncName_Substring_Index: SQLFuncName_Substring_Index,
		SQLFuncName_Date:            SQLFuncName_Date,
	}
)

type ExprField interface {
	GetType() SQLFieldType
	GetTable() string
	GetFunc() string
	GetDistinct() bool
	GetArgs() []ExprField
	GetColName() *Column
	GetColType() ColumnType
	GetName() string
	GetRawName() string
	GetAsName() string
	GetValueKind() byte
	GetValue() any
	SetType(SQLFieldType)
	SetTable(string)
	SetFunc(string)
	SetDistinct(bool)
	SetArgs([]ExprField)
	SetName(*Column)
	SetAsName(string)
	SetValueKind(byte)
	SetValue(any)
	HasDistinct() bool
	IsAllValue() bool
}

type SQLField struct {
	Type      SQLFieldType
	Table     *string
	Func      *string
	Distinct  bool
	Args      []ExprField
	Name      *Column
	AsName    *string
	ValueKind *byte
	Value     any
}

func (f *SQLField) GetColName() *Column {
	return f.Name
}

func (f *SQLField) GetRawName() string {
	if f.Name != nil {
		return f.Name.Name
	}
	return ""
}

func (f *SQLField) GetColType() ColumnType {
	if f.Name != nil {
		return f.Name.Type
	}
	return 0
}

func (f *SQLField) GetType() SQLFieldType {
	return f.Type
}

func (f *SQLField) GetTable() string {
	if f.Func != nil {
		return f.Args[0].GetTable()
	}
	if f.Table != nil {
		return *f.Table
	}
	return ""
}

func (f *SQLField) GetFunc() string {
	if f.Func != nil {
		return *f.Func
	}
	return ""
}

func (f *SQLField) GetDistinct() bool {
	return f.Distinct
}

func (f *SQLField) GetArgs() []ExprField {
	return f.Args
}

func (f *SQLField) GetValueKind() byte {
	if f.ValueKind != nil {
		return *f.ValueKind
	}
	return 0
}

func (f *SQLField) GetValue() any {
	return f.Value
}

func (f *SQLField) SetType(fieldType SQLFieldType) {
	f.Type = fieldType
}

func (f *SQLField) SetTable(table string) {
	f.Table = &table
}

func (f *SQLField) SetFunc(fn string) {
	f.Func = &fn
}

func (f *SQLField) SetDistinct(distinct bool) {
	f.Distinct = distinct
}

func (f *SQLField) SetArgs(args []ExprField) {
	f.Args = args
}

func (f *SQLField) SetName(name *Column) {
	f.Name = name
}

func (f *SQLField) SetAsName(asName string) {
	f.AsName = &asName
}

func (f *SQLField) SetValueKind(kind byte) {
	f.ValueKind = &kind
}

func (f *SQLField) SetValue(val any) {
	f.Value = val
}

func (f *SQLField) Copy(source *SQLField) {
	f.Type = source.Type
	f.Table = source.Table
	f.Func = source.Func
	f.Distinct = source.Distinct
	f.Args = source.Args
	f.Name = source.Name
	f.AsName = source.AsName
	f.ValueKind = source.ValueKind
	f.Value = source.Value
}

func (f SQLField) GetName() string {
	var name string
	switch f.Type {
	case SQLField_Type_Normal:
		name = f.Name.Name
	case SQLField_Type_Agg_Func, SQLField_Type_Func:
		var args []string
		for _, field := range f.Args {
			args = append(args, field.GetName())
		}
		name = fmt.Sprintf("%v(%v)", *f.Func, strings.Join(args, ","))
	case SQLField_Type_WildCard:
		name = SQLField_Type_WildCard_Value
	case SQLField_Type_Value:
		name = fmt.Sprint(f.Value)
	}

	if f.Table != nil {
		name = fmt.Sprintf("%v.%v", *f.Table, name)
	}

	return name
}

func (f SQLField) GetAsName() string {
	asName := f.GetName()
	if f.AsName != nil {
		asName = *f.AsName
	}
	return asName
}

func (f *SQLField) HasDistinct() bool {
	if f.Distinct {
		return f.Distinct
	}
	for _, arg := range f.Args {
		distinct := arg.HasDistinct()
		if distinct {
			return distinct
		}
	}
	return false
}

func (f *SQLField) IsAllValue() bool {
	if f.Type == SQLField_Type_Normal || f.Type == SQLField_Type_WildCard {
		return false
	}

	for _, arg := range f.Args {
		isAllValue := arg.IsAllValue()
		if !isAllValue {
			return isAllValue
		}
	}

	return true
}

type SQLTable struct {
	Name        *string
	AsName      *string
	TableSchema map[string][]*Column
	Ref         *SQLSelect
	Left        *SQLTable
	Right       *SQLTable
	On          ExprField
}

func (t SQLTable) GetAsName() string {
	if t.AsName != nil {
		return *t.AsName
	}
	if t.Name != nil {
		return *t.Name
	}
	return ""
}

type SQLOrderBy struct {
	SQLField
	Desc bool
}

type SQLLimit struct {
	Limit  *int64
	Offset *int64
}

type BinaryOperation struct {
	SQLField
	Operator *opcode.Op
	Left     ExprField
	Right    ExprField
	Not      bool
}

type CaseWhenExpr struct {
	SQLField
	WhenClauses []*WhenClause
	Else        ExprField
}

type WhenClause struct {
	Expr   ExprField
	Result ExprField
}

func (b *BinaryOperation) IsAllValue() bool {
	if isAllValue := b.Left.IsAllValue(); !isAllValue {
		return isAllValue
	}
	if isAllValue := b.Right.IsAllValue(); !isAllValue {
		return isAllValue
	}

	return true
}

func (b *BinaryOperation) GetArgs() []ExprField {
	args := []ExprField{b.Left, b.Right}
	return args
}

type ColumnType int

const (
	Int      ColumnType = 1
	Float    ColumnType = 2
	Boolean  ColumnType = 3
	String   ColumnType = 4
	Datetime ColumnType = 5
	Json     ColumnType = 6
	Array    ColumnType = 7
	WildCard ColumnType = 8
)

var (
	Func_Arg_Err_Msg      = "func %v must contains %v arg,but got %v arg"
	Func_Arg_Type_Err_Msg = "func %v have unexpect arg,type=%v,name=%v"
)

type Column struct {
	Type  ColumnType
	Name  string
	Array *Column
	Json  map[string]*Column
}

func NewMySQLSelectParser(sql string, tableSchema map[string][]*Column) SelectParser {
	return &MySQLSelectParser{sql: sql, parser: tiParser.New(), tableSchema: tableSchema}
}

func (parser *MySQLSelectParser) Parse() (sql SQL, err error) {
	err = parser.parse()
	if err != nil {
		err = fmt.Errorf("parse sql failed,err=[%v],sql=[%v]", err.Error(), parser.sql)
		return
	}

	err = parser.validate()
	if err != nil {
		err = fmt.Errorf("validate sql failed,err=[%v],sql=[%v]", err.Error(), parser.sql)
		return
	}

	sql = parser.sqlSelect

	return
}

func (parser *MySQLSelectParser) parse() (err error) {
	log.Debugf("original sql is [%v]", parser.sql)

	var stmts []ast.StmtNode
	stmts, _, err = parser.parser.Parse(parser.sql, "", "")
	if err != nil {
		return
	}
	if len(stmts) == 0 {
		err = fmt.Errorf("parse empty sql=%v", parser.sql)
		return
	}
	stmt := stmts[0]
	if selectStmt, ok := stmt.(*ast.SelectStmt); ok {
		parser.ast = selectStmt
	} else {
		err = fmt.Errorf("not select sql=%v", parser.sql)
		return
	}

	parser.sqlSelect, err = parseSelectStmt(parser.ast, map[string]bool{})
	if err != nil {
		return
	}

	parser.sqlSelect.SQL = &parser.sql

	// 设置tableSchema
	err = populateTableSchema(parser.sqlSelect.From, parser.tableSchema)
	if err != nil {
		return
	}

	return
}

func populateTableSchema(table *SQLTable, tableSchema map[string][]*Column) (err error) {
	if table == nil {
		return
	}
	if table.Ref != nil {
		err = populateTableSchema(table.Ref.From, tableSchema)
		if err != nil {
			return
		}
	} else {
		err = populateTableSchema(table.Left, tableSchema)
		if err != nil {
			return
		}
		err = populateTableSchema(table.Right, tableSchema)
		if err != nil {
			return
		}
	}

	if table.Name != nil {
		if schema, ok := tableSchema[*table.Name]; ok {
			table.TableSchema = map[string][]*Column{table.GetAsName(): schema}
		} else {
			err = fmt.Errorf("invalid table name=%v,valid tables=%v", *table.Name, maps.Keys(tableSchema))
		}
	} else if table.Ref == nil {
		schema := map[string][]*Column{}
		if table.Left != nil {
			for k, v := range table.Left.TableSchema {
				schema[k] = v
			}
		}
		if table.Right != nil {
			for k, v := range table.Right.TableSchema {
				schema[k] = v
			}
		}
		table.TableSchema = schema
	} else {
		var fields []ExprField
		var wildCardField ExprField
		for _, field := range table.Ref.Fields {
			if field.GetType() == SQLField_Type_WildCard {
				wildCardField = field
				continue
			}
			fields = append(fields, field)
		}

		if wildCardField != nil {
			// 通配符查询时schema为所有字段，需要校验字段是否重复
			var allCols []*Column
			colMap := map[string]string{}
			for tableName, cols := range table.Ref.From.TableSchema {
				if wildCardField.GetTable() != "" && wildCardField.GetTable() != tableName {
					// 指定表名的通配符查询只检查该表字段
					allCols = append(allCols, cols...)
					continue
				}
				for _, col := range cols {
					if ot, ok := colMap[col.Name]; ok {
						err = fmt.Errorf("select duplicate column [%v] in tables [%v,%v]", col.Name, ot, tableName)
					} else {
						colMap[col.Name] = tableName
						allCols = append(allCols, col)
					}
				}
			}
			table.TableSchema = map[string][]*Column{table.GetAsName(): allCols}
		}

		if table.TableSchema == nil {
			table.TableSchema = map[string][]*Column{}
		}

		for _, field := range fields {
			// 通配符查询时，普通字段无需重复添加
			if wildCardField != nil && wildCardField.GetTable() == field.GetTable() && field.GetType() == SQLField_Type_Normal {
				continue
			}
			var col *Column
			col, err = findColFromSchema(field, table.Ref.From.TableSchema)
			if err != nil {
				return
			}
			table.TableSchema[table.GetAsName()] = append(table.TableSchema[table.GetAsName()], &Column{
				Type: col.Type,
				Name: field.GetAsName(),
			})
		}
	}

	return
}

func parseSelectStmt(stmtNode *ast.SelectStmt, tableAlias map[string]bool) (sqlSelecct *SQLSelect, err error) {
	var fields []ExprField
	for _, selectField := range stmtNode.Fields.Fields {
		var field ExprField
		field, err = parserSelectField(selectField)
		if err != nil {
			return
		}
		if field != nil {
			if stmtNode.Distinct && field.GetType() != SQLField_Type_Value {
				field.SetDistinct(stmtNode.Distinct)
			}
			fields = append(fields, field)
		}
	}

	var where ExprField
	if stmtNode.Where != nil {
		where, err = parserFieldExpr(stmtNode.Where)
		if err != nil {
			return
		}
	}

	var groupBy []ExprField
	if stmtNode.GroupBy != nil {
		for _, item := range stmtNode.GroupBy.Items {
			var field ExprField
			field, err = parserFieldExpr(item.Expr)
			if err != nil {
				return
			}
			groupBy = append(groupBy, field)
		}
	}

	var having ExprField
	if stmtNode.Having != nil {
		having, err = parserFieldExpr(stmtNode.Having.Expr)
		if err != nil {
			return
		}
	}

	var orderBy []*SQLOrderBy
	if stmtNode.OrderBy != nil {
		for _, item := range stmtNode.OrderBy.Items {
			var field ExprField
			field, err = parserFieldExpr(item.Expr)
			if err != nil {
				return
			}
			orderBy = append(orderBy, &SQLOrderBy{
				SQLField: *field.(*SQLField),
				Desc:     item.Desc,
			})
		}
	}

	var limit *SQLLimit
	if stmtNode.Limit != nil {
		var lf ExprField
		lf, err = parserFieldExpr(stmtNode.Limit.Count)
		if err != nil {
			return
		}
		limitCnt := lf.GetValue().(int64)
		limit = &SQLLimit{
			Limit: &limitCnt,
		}
		if stmtNode.Limit.Offset != nil {
			var offset ExprField
			offset, err = parserFieldExpr(stmtNode.Limit.Offset)
			if err != nil {
				return
			}
			offsetCnt := offset.GetValue().(int64)
			limit.Offset = &offsetCnt
		}
	}

	var from *SQLTable
	from, err = parseTableClause(stmtNode.From.TableRefs, tableAlias)
	if err != nil {
		return
	}

	sqlSelecct = &SQLSelect{
		Distinct: stmtNode.Distinct,
		Fields:   fields,
		From:     from,
		Where:    where,
		GroupBy:  groupBy,
		Having:   having,
		OrderBy:  orderBy,
		Limit:    limit,
	}

	return
}

func parseTableClause(rsNode ast.ResultSetNode, tableAlias map[string]bool) (table *SQLTable, err error) {
	if rsNode == nil {
		return
	}
	if join, ok := rsNode.(*ast.Join); ok {
		var on ExprField
		if join.On != nil {
			on, err = parserFieldExpr(join.On.Expr)
			if err != nil {
				return
			}
		}
		var left *SQLTable
		left, err = parseTableClause(join.Left, tableAlias)
		if err != nil {
			return
		}
		var right *SQLTable
		right, err = parseTableClause(join.Right, tableAlias)
		if err != nil {
			return
		}

		if left != nil && right != nil && on == nil {
			// 表连接必须包含连接条件
			err = fmt.Errorf("missing on condition in table join")
			return
		}

		table = &SQLTable{
			Left:  left,
			Right: right,
			On:    on,
		}
		return
	}

	if tableSource, ok := rsNode.(*ast.TableSource); ok {
		table = &SQLTable{}
		if tableSource.AsName.O != "" {
			table.AsName = &tableSource.AsName.O
			if ok := tableAlias[*table.AsName]; ok {
				err = fmt.Errorf("can't have duplicate table alias [%v]", *table.AsName)
				return
			} else {
				tableAlias[*table.AsName] = true
			}
		}
		if tableName, ok := tableSource.Source.(*ast.TableName); ok {
			table.Name = &tableName.Name.O
		}
		if tableRef, ok := tableSource.Source.(*ast.SelectStmt); ok {
			if table.AsName == nil {
				err = fmt.Errorf("subSelect must have alias")
				return
			}
			var selectStmt *SQLSelect
			selectStmt, err = parseSelectStmt(tableRef, map[string]bool{})
			if err != nil {
				return
			}
			table.Ref = selectStmt
		}
		return
	} else {
		log.Debugf("unknown ResultSetNode type=%T", rsNode)
	}

	return
}

func parserBinaryOperationExpr(exprNode ast.ExprNode) (bo ExprField, err error) {
	if subBo, ok := exprNode.(*ast.BinaryOperationExpr); ok {
		var left ExprField
		left, err = parserBinaryOperationExpr(subBo.L)
		if err != nil {
			return
		}
		var right ExprField
		right, err = parserBinaryOperationExpr(subBo.R)
		if err != nil {
			return
		}

		bo = &BinaryOperation{
			Operator: &subBo.Op,
			Left:     left,
			Right:    right,
		}
		return
	}

	if subBo, ok := exprNode.(*ast.ParenthesesExpr); ok {
		bo, err = parserBinaryOperationExpr(subBo.Expr)
		return
	}

	bo, err = parserFieldExpr(exprNode)

	return
}

func parserSelectField(selectField *ast.SelectField) (field ExprField, err error) {
	// 通配符查询
	if selectField.WildCard != nil {
		field = &SQLField{Type: SQLField_Type_WildCard, Name: &Column{Type: WildCard, Name: SQLField_Type_WildCard_Value}}
		if selectField.WildCard.Table.O != "" {
			field.SetTable(selectField.WildCard.Table.O)
		}
		return
	}

	field, err = parserFieldExpr(selectField.Expr)
	if err != nil {
		return
	}
	if field == nil {
		return
	}

	// 字段别名
	if selectField.AsName.O != "" {
		field.SetAsName(selectField.AsName.O)
	}

	return
}

func parserFieldExpr(node ast.ExprNode) (field ExprField, err error) {
	field = &SQLField{}
	switch exprNode := node.(type) {
	// 普通字段
	case *ast.ColumnNameExpr:
		field.SetType(SQLField_Type_Normal)
		if exprNode.Name.Table.O != "" {
			field.SetTable(exprNode.Name.Table.O)
		}
		if exprNode.Name.Name.O != "" {
			field.SetName(&Column{Name: exprNode.Name.Name.O})
		}
	// 聚合函数的字段
	case *ast.AggregateFuncExpr:
		field.SetType(SQLField_Type_Agg_Func)
		if f, ok := SQLFunc_Agg_Map[strings.ToUpper(exprNode.F)]; ok {
			field.SetFunc(f)
		} else {
			err = fmt.Errorf("invalid function name=%v", exprNode.F)
			return
		}
		field.SetDistinct(exprNode.Distinct)
		var argFields []ExprField
		for _, arg := range exprNode.Args {
			var argField ExprField
			argField, err = parserFieldExpr(arg)
			if err != nil {
				return
			}
			if argField != nil {
				if field.GetDistinct() && argField.GetType() != SQLField_Type_Value {
					argField.SetDistinct(field.GetDistinct())
				}
				argFields = append(argFields, argField)
			}
		}
		field.SetArgs(argFields)
	// 取值函数的字段
	case *ast.FuncCallExpr:
		field.SetType(SQLField_Type_Func)
		if f, ok := SQLFunc_Map[strings.ToUpper(exprNode.FnName.O)]; ok {
			field.SetFunc(f)
		} else {
			err = fmt.Errorf("invalid function name=%v", exprNode.FnName.O)
			return
		}
		var argFields []ExprField
		for _, arg := range exprNode.Args {
			var argField ExprField
			argField, err = parserFieldExpr(arg)
			if err != nil {
				return
			}
			if argField != nil {
				argFields = append(argFields, argField)
			}
		}
		field.SetArgs(argFields)
	// 显式值
	case *parserDriver.ValueExpr:
		field.SetType(SQLField_Type_Value)
		var value any
		var kind byte
		var name *Column
		switch exprNode.Datum.Kind() {
		case types.KindNull:
			kind = types.KindNull
			value = nil
			name = &Column{Type: String, Name: fmt.Sprint(nil)}
		case types.KindInt64:
			kind = types.KindInt64
			value = exprNode.Datum.GetInt64()
			name = &Column{Type: Int, Name: fmt.Sprint(value)}
		case types.KindUint64:
			kind = types.KindUint64
			value = int64(exprNode.Datum.GetUint64())
			name = &Column{Type: Int, Name: fmt.Sprint(value)}
		case types.KindFloat64:
			kind = types.KindFloat64
			value = exprNode.Datum.GetFloat64()
			name = &Column{Type: Float, Name: fmt.Sprint(value)}
		case types.KindMysqlDecimal:
			kind = types.KindMysqlDecimal
			value, _ = exprNode.Datum.GetMysqlDecimal().ToFloat64()
			name = &Column{Type: Float, Name: fmt.Sprint(value)}
		case types.KindString:
			kind = types.KindString
			value = exprNode.Datum.GetString()
			name = &Column{Type: String, Name: fmt.Sprint(value)}
		default:
			log.Debugf("value kind=%v", exprNode.Datum.Kind())
		}
		field.SetValueKind(kind)
		field.SetValue(value)
		field.SetName(name)
	// 带计算符的字段
	case *ast.BinaryOperationExpr:
		field, err = parserBinaryOperationExpr(exprNode)
	case *ast.ParenthesesExpr:
		field, err = parserFieldExpr(exprNode.Expr)
	case *ast.PatternInExpr:
		operator := opcode.In
		bo := BinaryOperation{Operator: &operator, Not: exprNode.Not}
		bo.Left, err = parserFieldExpr(exprNode.Expr)
		if err != nil {
			return
		}
		var argFields []ExprField
		for _, arg := range exprNode.List {
			var argField ExprField
			argField, err = parserFieldExpr(arg)
			if err != nil {
				return
			}
			argFields = append(argFields, argField)
		}
		bo.Right = &SQLField{Args: argFields}
		field = &bo
	case *ast.CaseExpr:
		caseWhen := CaseWhenExpr{SQLField: SQLField{Type: SQLField_Type_Func}}
		caseWhen.SetFunc(SQLFuncName_Case)
		if exprNode.ElseClause != nil {
			caseWhen.Else, err = parserFieldExpr(exprNode.ElseClause)
			if err != nil {
				return
			}
		}
		var whenClauses []*WhenClause
		for _, whenClause := range exprNode.WhenClauses {
			var expr ExprField
			expr, err = parserFieldExpr(whenClause.Expr)
			if err != nil {
				return
			}
			var result ExprField
			result, err = parserFieldExpr(whenClause.Result)
			if err != nil {
				return
			}
			whenClauses = append(whenClauses, &WhenClause{Expr: expr, Result: result})
		}
		caseWhen.WhenClauses = whenClauses
		field = &caseWhen
	default:
		err = fmt.Errorf("unknow exprNode type=%T", node)
		return
	}

	return
}

func (parser *MySQLSelectParser) validate() (err error) {
	// 语句中的字段必须有效
	err = validateField(parser.sqlSelect)
	if err != nil {
		return
	}

	// 语句中的函数必须有效
	err = validateFunc(parser.sqlSelect)
	if err != nil {
		return
	}

	return
}

func validateFunc(sqlSelect *SQLSelect) (err error) {
	// 校验嵌套子查询
	if sqlSelect.From.Left != nil && sqlSelect.From.Left.Ref != nil {
		err = validateFunc(sqlSelect.From.Left.Ref)
		if err != nil {
			return
		}
	}
	if sqlSelect.From.Right != nil && sqlSelect.From.Right.Ref != nil {
		err = validateFunc(sqlSelect.From.Right.Ref)
		if err != nil {
			return
		}
	}

	// 校验查询函数是否合法
	var aggFuncWithoutGroupBy bool
	allAggFunc := true
	for _, field := range sqlSelect.Fields {
		if len(sqlSelect.GroupBy) == 0 {
			if field.GetType() == SQLField_Type_Agg_Func {
				aggFuncWithoutGroupBy = true
			} else {
				allAggFunc = false
			}
		}
	}

	if aggFuncWithoutGroupBy && !allAggFunc {
		// 只能全为聚合函数
		err = fmt.Errorf("can't use aggregate function without GROUP BY clause")
		return
	}

	// 校验过滤函数是否合法
	var whereFields []ExprField
	if sqlSelect.Where != nil {
		whereFields = collectBinaryOperationFields(sqlSelect.Where)
	}
	for _, field := range whereFields {
		if field.GetType() == SQLField_Type_Agg_Func {
			err = fmt.Errorf("[%v] use aggregate function in WHERE clause", field.GetRawName())
			return
		}
	}

	// 校验分组函数是否合法
	for _, field := range sqlSelect.GroupBy {
		if field.GetType() == SQLField_Type_Agg_Func {
			err = fmt.Errorf("[%v] use aggregate function in GROUP BY clause", field.GetRawName())
			return
		}
	}

	// 校验分组过滤函数是否合法
	var havingFields []ExprField
	if sqlSelect.Having != nil {
		havingFields = collectBinaryOperationFields(sqlSelect.Having)
	}
	for _, field := range havingFields {
		if field.GetType() == SQLField_Type_Agg_Func {
			err = fmt.Errorf("[%v] use aggregate function in HAVING clause", field.GetRawName())
			return
		}
	}

	// 校验排序函数是否合法
	var orderByFields []*SQLField
	for idx := range sqlSelect.OrderBy {
		orderByFields = append(orderByFields, &sqlSelect.OrderBy[idx].SQLField)
	}
	for _, field := range orderByFields {
		if field.Type == SQLField_Type_Agg_Func {
			err = fmt.Errorf("[%v] use aggregate function in ORDER BY clause", field.Name.Name)
			return
		}
	}

	return
}

func validateField(sqlSelect *SQLSelect) (err error) {
	// 校验嵌套子查询
	if sqlSelect.From.Ref != nil {
		err = validateField(sqlSelect.From.Ref)
		if err != nil {
			return
		}
	}
	if sqlSelect.From.Left != nil && sqlSelect.From.Left.Ref != nil {
		err = validateField(sqlSelect.From.Left.Ref)
		if err != nil {
			return
		}
	}
	if sqlSelect.From.Right != nil && sqlSelect.From.Right.Ref != nil {
		err = validateField(sqlSelect.From.Right.Ref)
		if err != nil {
			return
		}
	}

	// 查询字段不能重复
	err = validateDuplicateSelectField(sqlSelect)
	if err != nil {
		return
	}
	// 校验查询字段有效性
	for _, field := range sqlSelect.Fields {
		var col *Column
		col, err = findColFromSchema(field, sqlSelect.From.TableSchema)
		if err != nil {
			return
		}
		field.SetName(col)
	}

	// 校验过滤字段
	var whereFields []ExprField
	if sqlSelect.Where != nil {
		whereFields = collectBinaryOperationFields(sqlSelect.Where)
	}
	for _, field := range whereFields {
		var col *Column
		col, err = findColFromSchema(field, sqlSelect.From.TableSchema)
		if err != nil {
			return
		}
		field.SetName(col)
	}

	if len(sqlSelect.GroupBy) > 0 {
		// 存在分组字段时，select只能是聚合函数或者group by中的字段
		var groupByFields []string
		for _, field := range sqlSelect.GroupBy {
			var col *Column
			col, err = findColFromSchema(field, sqlSelect.From.TableSchema)
			if err != nil {
				var found bool
				// group by字段可能是select字段
				for _, selectField := range sqlSelect.Fields {
					if field.GetName() == selectField.GetAsName() {
						field.(*SQLField).Copy(selectField.(*SQLField))
						found = true
						err = nil
						break
					}
				}
				if found {
					continue
				}
				return
			}
			field.SetName(col)
			groupByFields = append(groupByFields, field.GetName())
		}
		err = validateFieldWhenHaveGroupBy(groupByFields, sqlSelect.Fields)
		if err != nil {
			return
		}
	}

	// 校验分组过滤字段
	var havingFields []ExprField
	if sqlSelect.Having != nil {
		havingFields = collectBinaryOperationFields(sqlSelect.Having)
	}
	for _, field := range havingFields {
		var col *Column
		col, err = findColFromSchema(field, sqlSelect.From.TableSchema)
		if err != nil {
			var found bool
			// having字段可能是select字段
			for _, selectField := range sqlSelect.Fields {
				if field.GetName() == selectField.GetAsName() {
					found = true
					err = nil
					break
				}
			}
			if found {
				continue
			}
			return
		}
		field.SetName(col)
	}

	// 校验排序字段
	var orderByFields []*SQLField
	for idx := range sqlSelect.OrderBy {
		orderByFields = append(orderByFields, &sqlSelect.OrderBy[idx].SQLField)
	}
	for _, field := range orderByFields {
		var col *Column
		col, err = findColFromSchema(field, sqlSelect.From.TableSchema)
		if err != nil {
			var found bool
			// order by字段可能是select字段
			for _, selectField := range sqlSelect.Fields {
				if field.GetName() == selectField.GetAsName() {
					found = true
					err = nil
					break
				}
			}
			if found {
				continue
			}
			return
		}
		field.Name = col
	}

	if sqlSelect.From.On != nil {
		// 校验表连接字段
		onFields := collectBinaryOperationFields(sqlSelect.From.On)
		for _, field := range onFields {
			var col *Column
			col, err = findColFromSchema(field, sqlSelect.From.TableSchema)
			if err != nil {
				return
			}
			field.SetName(col)
		}
	}

	return
}

func validateFieldWhenHaveGroupBy(groupByFields []string, fields []ExprField) (err error) {
	if len(groupByFields) == 0 || len(fields) == 0 {
		return
	}
	for _, field := range fields {
		switch field.GetType() {
		case SQLField_Type_Value, SQLField_Type_Agg_Func:
			// 显式值、聚合函数不限制
			continue
		case SQLField_Type_WildCard:
			// 不允许通配符
			err = fmt.Errorf("can't use * when have groupBy")
			return
		case SQLField_Type_Normal:
			// 普通字段需保证字段在grgouBy中
			if !slices.Contains(groupByFields, field.GetName()) {
				err = fmt.Errorf("field %v not in groupBy %v", field.GetName(), groupByFields)
				return
			}
		case SQLField_Type_Func:
			// 使用json_extract时需保证select与group by一致
			if field.GetFunc() == SQLFuncName_Json_Extract {
				if !slices.Contains(groupByFields, field.GetName()) {
					err = fmt.Errorf("field %v not in groupBy %v", field.GetName(), groupByFields)
					return
				}
			} else {
				// 先直接判断名称是否一致
				if slices.Contains(groupByFields, field.GetName()) {
					continue
				}
				// 不一致则递归校验参数
				err = validateFieldWhenHaveGroupBy(groupByFields, field.GetArgs())
				if err != nil {
					return
				}
			}
		}
	}

	return
}

func findColFromSchema(field ExprField, tableSchema map[string][]*Column) (matchCol *Column, err error) {
	if field.GetType() == SQLField_Type_Value {
		matchCol = field.GetColName()
		return
	}
	if field.GetType() == SQLField_Type_WildCard {
		matchCol = &Column{
			Type: WildCard,
			Name: SQLField_Type_WildCard_Value,
		}
		return
	}

	// 函数字段
	if field.GetType() == SQLField_Type_Func || field.GetType() == SQLField_Type_Agg_Func {
		var cols []*Column
		for _, arg := range field.GetArgs() {
			var col *Column
			col, err = findColFromSchema(arg, tableSchema)
			if err != nil {
				return
			}
			cols = append(cols, col)
		}
		switch field.GetFunc() {
		case SQLFuncName_Sum, SQLFuncName_Max, SQLFuncName_Min:
			if len(cols) != 1 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 1, len(cols))
				return
			}
			col := cols[0]
			if slices.Contains([]ColumnType{WildCard}, col.Type) {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}
			matchCol = &Column{Type: col.Type, Name: field.GetAsName()}
		case SQLFuncName_Avg:
			if len(cols) != 1 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 1, len(cols))
				return
			}
			col := cols[0]
			if !slices.Contains([]ColumnType{Int, Float}, col.Type) {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}
			matchCol = &Column{Type: Float, Name: field.GetAsName()}
		case SQLFuncName_Count:
			if len(cols) != 1 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 1, len(cols))
				return
			}
			matchCol = &Column{Type: Int, Name: field.GetAsName()}
		case SQLFuncName_Json_Extract:
			if len(cols) != 2 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 2, len(cols))
				return
			}
			col := cols[0]
			expr := cols[1]
			if col.Type != Json {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}
			if expr.Type != String {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}

			matchCol, err = findColumnFromJson(*col, expr)
			if err != nil {
				return
			}
			matchCol = &Column{Type: matchCol.Type, Name: field.GetAsName()}
		case SQLFuncName_Year, SQLFuncName_Month, SQLFuncName_Day, SQLFuncName_Hour, SQLFuncName_Minute, SQLFuncName_Second:
			if len(cols) != 1 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 1, len(cols))
				return
			}
			col := cols[0]
			if !slices.Contains([]ColumnType{Datetime}, col.Type) {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}
			matchCol = &Column{Type: Int, Name: field.GetAsName()}
		case SQLFuncName_Date_Format:
			if len(cols) != 2 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 2, len(cols))
				return
			}
			datetimeCol := cols[0]
			if !slices.Contains([]ColumnType{Datetime}, datetimeCol.Type) {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), datetimeCol.Type, datetimeCol.Name)
				return
			}
			patternFiled := cols[1]
			if patternFiled.Type != String {
				err = fmt.Errorf("datetime format pattern must be string")
				return
			}
			matchCol = &Column{Type: String, Name: field.GetAsName()}
		case SQLFuncName_Round:
			if len(cols) != 2 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 2, len(cols))
				return
			}
			numberCol := cols[0]
			if !slices.Contains([]ColumnType{Int, Float}, numberCol.Type) {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), numberCol.Type, numberCol.Name)
				return
			}
			preciseCol := cols[1]
			if preciseCol.Type != Int {
				err = fmt.Errorf("round precise must be int")
				return
			}
			matchCol = &Column{Type: Float, Name: field.GetAsName()}
		case SQLFuncName_If:
			// IF函数必须有三个参数
			if len(cols) != 3 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 3, len(cols))
				return
			}
			// 第一个参数必须是布尔值或int值
			exprCol := cols[0]
			if exprCol.Type != Boolean && exprCol.Type != Int {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), exprCol.Type, exprCol.Name)
				return
			}
			matchCol = &Column{Type: cols[1].Type, Name: field.GetAsName()}
		case SQLFuncName_Case:
			caseWhen := field.(*CaseWhenExpr)
			var colType ColumnType
			for _, whenClause := range caseWhen.WhenClauses {
				var whenExprCol *Column
				whenExprCol, err = findColFromSchema(whenClause.Expr, tableSchema)
				if err != nil {
					return
				}
				whenClause.Expr.SetName(whenExprCol)
				var whenResultCol *Column
				whenResultCol, err = findColFromSchema(whenClause.Result, tableSchema)
				if err != nil {
					return
				}
				whenClause.Result.SetName(whenResultCol)
				if whenClause.Result.GetType() == SQLField_Type_Value {
					if whenClause.Result.GetValue() != nil {
						// 非空值才能判断类型
						colType = whenResultCol.Type
					}
				} else {
					colType = whenResultCol.Type
				}
			}
			matchCol = &Column{Type: colType, Name: field.GetAsName()}
		case SQLFuncName_Now:
			matchCol = &Column{Type: Datetime, Name: field.GetAsName()}
		case SQLFuncName_To_Double:
			if len(cols) != 1 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 1, len(cols))
				return
			}
			col := cols[0]
			if !slices.Contains([]ColumnType{String}, col.Type) {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}
			matchCol = &Column{Type: Float, Name: field.GetAsName()}
		case SQLFuncName_To_Floor:
			if len(cols) != 1 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 1, len(cols))
				return
			}
			col := cols[0]
			if !slices.Contains([]ColumnType{Int, Float}, col.Type) {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}
			matchCol = &Column{Type: Int, Name: field.GetAsName()}
		case SQLFuncName_Json_Arr_Agg:
			if len(cols) != 1 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 1, len(cols))
				return
			}
			col := cols[0]
			if slices.Contains([]ColumnType{WildCard}, col.Type) {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}
			matchCol = &Column{Type: Array, Name: field.GetAsName(), Array: &Column{Type: col.Type}}
		case SQLFuncName_Substring_Index:
			if len(cols) != 3 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 3, len(cols))
				return
			}
			if cols[0].Type != String || cols[1].Type != String || cols[2].Type != Int {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), cols, cols)
				return
			}
			matchCol = &Column{Type: String, Name: field.GetAsName()}
		case SQLFuncName_Date:
			if len(cols) != 1 {
				err = fmt.Errorf(Func_Arg_Err_Msg, field.GetFunc(), 1, len(cols))
				return
			}
			col := cols[0]
			if col.Type != Datetime {
				err = fmt.Errorf(Func_Arg_Type_Err_Msg, field.GetFunc(), col.Type, col.Name)
				return
			}
			matchCol = &Column{Type: Datetime, Name: field.GetAsName()}
		default:
			err = fmt.Errorf("unsupport func [%v]", field.GetFunc())
			return
		}
		return
	}

	// 二元表达式
	if bo, ok := field.(*BinaryOperation); ok {
		var left *Column
		left, err = findColFromSchema(bo.Left, tableSchema)
		if err != nil {
			return
		}
		bo.Left.SetName(left)
		var right *Column
		if *bo.Operator == opcode.In {
			var argCols []*Column
			for _, arg := range bo.Right.GetArgs() {
				var argCol *Column
				argCol, err = findColFromSchema(arg, tableSchema)
				if err != nil {
					return
				}
				argCols = append(argCols, argCol)
			}
			right = &Column{Type: argCols[0].Type}
		} else {
			right, err = findColFromSchema(bo.Right, tableSchema)
			if err != nil {
				return
			}
		}
		bo.Right.SetName(right)

		switch *bo.Operator {
		case opcode.Plus, opcode.Minus, opcode.Mul, opcode.Div:
			if !slices.Contains([]ColumnType{Int, Float, Datetime}, left.Type) || !slices.Contains([]ColumnType{Int, Float, Datetime}, right.Type) {
				err = fmt.Errorf("only number/date column can perform [+-*/]")
				return
			}

			colType := left.Type
			if right.Type == Float {
				colType = Float
			}
			if *bo.Operator == opcode.Div {
				colType = Float
			}

			matchCol = &Column{Type: colType, Name: field.GetAsName()}
		case opcode.GE, opcode.GT, opcode.LE, opcode.LT, opcode.EQ, opcode.LogicAnd,
			opcode.LogicOr, opcode.And, opcode.Or, opcode.In, opcode.NE:
			matchCol = &Column{Type: Boolean, Name: field.GetAsName()}
		default:
			err = fmt.Errorf("unsupport operator [%v]", *bo.Operator)
			return
		}
		return
	}

	// 声明了表别名则从指定schema中获取
	if field.GetTable() != "" {
		if _, ok := tableSchema[field.GetTable()]; !ok {
			err = fmt.Errorf("unkown table alias,field=%v", field.GetName())
			return
		}
		for _, col := range tableSchema[field.GetTable()] {
			if col.Name == field.GetRawName() {
				matchCol = col
				break
			}
		}
		if matchCol == nil {
			err = fmt.Errorf("unkown field=%v", field.GetName())
			return
		}
		return
	}

	// 未声明表名则遍历所有表校验：1）是否存在；2）是否存在于多个表中
	matchTable := map[string]*Column{}
	for table, cols := range tableSchema {
		for _, col := range cols {
			if col.Name == field.GetRawName() {
				matchTable[table] = col
				break
			}
		}

	}
	if len(matchTable) == 0 {
		err = fmt.Errorf("unkown field=%v", field.GetName())
		return
	}
	// 多表中存在同名字段
	if len(matchTable) > 1 {
		err = fmt.Errorf("ambiguous field(%v) existed in multiple tables %v", field.GetName(), maps.Keys(matchTable))
		return
	}

	matchCol = maps.Values(matchTable)[0]

	return
}

func findColumnFromJson(col Column, expr *Column) (matchCol *Column, err error) {
	extractExpr := expr.Name
	if !strings.HasPrefix(extractExpr, "$.") {
		err = fmt.Errorf("invalid JSON_EXTRACT expr=%v", extractExpr)
		return
	}
	pathes := strings.Split(extractExpr, ".")
	for i := 1; i < len(pathes); i++ {
		path := pathes[i]
		if strings.Contains(path, "[") {
			// 是否为数组
			if col.Type != Array {
				err = fmt.Errorf("invalid JSON_EXTRACT expr=%v", extractExpr)
				return
			}
			col = *col.Array
			continue
		}
		// 是否为Json
		if col.Type != Json {
			err = fmt.Errorf("invalid col type(%v) to JSON_EXTRACT,expr=%v", col.Type, extractExpr)
			return
		}
		if subCol, ok := col.Json[path]; ok {
			col = *subCol
			continue
		}
		err = fmt.Errorf("invalid JSON_EXTRACT expr=%v,can't extract [.%v]", extractExpr, path)
		return
	}

	matchCol = &col

	return
}

func collectBinaryOperationFields(binaryOperation ExprField) (fields []ExprField) {
	if binaryOperation == nil {
		return
	}

	if bo, ok := binaryOperation.(*BinaryOperation); ok {
		fields = append(fields, collectBinaryOperationFields(bo.Left)...)
		if *bo.Operator == opcode.In {
			fields = append(fields, bo.Right.GetArgs()...)
		} else {
			fields = append(fields, collectBinaryOperationFields(bo.Right)...)
		}
	} else {
		fields = append(fields, binaryOperation)
	}

	return
}

func validateDuplicateSelectField(sqlSelect *SQLSelect) (err error) {
	selectFields := map[string]bool{}
	for _, field := range sqlSelect.Fields {
		if field.GetType() == SQLField_Type_WildCard {
			var cols []*Column
			if field.GetTable() != "" {
				if _, ok := sqlSelect.From.TableSchema[field.GetTable()]; ok {
					cols = sqlSelect.From.TableSchema[field.GetTable()]
				} else {
					err = fmt.Errorf("invalid table alias=%v", field.GetTable())
					return
				}
			} else {
				for _, v := range sqlSelect.From.TableSchema {
					cols = append(cols, v...)
				}
			}
			for _, col := range cols {
				if ok := selectFields[col.Name]; ok {
					err = fmt.Errorf("duplicate colName=%v,select column=%v", col.Name, field.GetAsName())
					return
				}
				selectFields[field.GetAsName()] = true
			}
			continue
		}
		if ok := selectFields[field.GetAsName()]; ok {
			err = fmt.Errorf("duplicate select asName=%v", field.GetAsName())
			return
		}
		selectFields[field.GetAsName()] = true
	}

	return
}

func (parser *MySQLSelectParser) GetOriginalSQL() string {
	return parser.sql
}

func (parser *MySQLSelectParser) GetStmt() (sqlSelect *SQLSelect, err error) {
	if parser.sqlSelect == nil {
		_, err = parser.Parse()
		if err != nil {
			return
		}
	}
	sqlSelect = parser.sqlSelect
	return
}
