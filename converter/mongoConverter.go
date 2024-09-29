package converter

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/tidb/parser/opcode"
	log "github.com/sirupsen/logrus"
	"github.com/tsfans/sql-to-mongo/parser"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/exp/maps"
)

const (
	Mongo_Stage_Lookup       = "$lookup"
	Mongo_Stage_Group        = "$group"
	Mongo_Stage_Replace_Root = "$replaceRoot"
	Mongo_Stage_Unwind       = "$unwind"
	Mongo_Stage_Macth        = "$match"
	Mongo_Stage_Sort         = "$sort"
	Mongo_Stage_Skip         = "$skip"
	Mongo_Stage_Limit        = "$limit"
	Mongo_Stage_AddFields    = "$addFields"

	Mongo_Operator_Gte          = "$gte"
	Mongo_Operator_Gt           = "$gt"
	Mongo_Operator_Lte          = "$lte"
	Mongo_Operator_Lt           = "$lt"
	Mongo_Operator_Eq           = "$eq"
	Mongo_Operator_Ne           = "$ne"
	Mongo_Operator_And          = "$and"
	Mongo_Operator_Or           = "$or"
	Mongo_Operator_In           = "$in"
	Mongo_Operator_Not          = "$not"
	Mongo_Operator_Expr         = "$expr"
	Mongo_Operator_Regex        = "$regex"
	Mongo_Operator_Sum          = "$sum"
	Mongo_Operator_Cond         = "$cond"
	Mongo_Operator_IfNull       = "$ifNull"
	Mongo_Operator_Avg          = "$avg"
	Mongo_Operator_AddToSet     = "$addToSet"
	Mongo_Operator_Size         = "$size"
	Mongo_Operator_Year         = "$year"
	Mongo_Operator_Month        = "$month"
	Mongo_Operator_DayOfMonth   = "$dayOfMonth"
	Mongo_Operator_Hour         = "$hour"
	Mongo_Operator_Minute       = "$minute"
	Mongo_Operator_Second       = "$second"
	Mongo_Operator_DateToString = "$dateToString"
	Mongo_Operator_Add          = "$add"
	Mongo_Operator_Subtract     = "$subtract"
	Mongo_Operator_Multiply     = "$multiply"
	Mongo_Operator_Divide       = "$divide"
	Mongo_Operator_Round        = "$round"
	Mongo_Operator_Switch       = "$switch"
	Mongo_Operator_Type         = "$type"
	Mongo_Operator_Now          = "$$NOW"
	Mongo_Operator_To_Double    = "$toDouble"
	Mongo_Operator_Floor        = "$floor"
	Mongo_Operator_Push         = "$push"
	Mongo_Operator_ArrayElemAt  = "$arrayElemAt"
	Mongo_Operator_Split        = "$split"

	Mongo_Arg_If       = "if"
	Mongo_Arg_Then     = "then"
	Mongo_Arg_Else     = "else"
	Mongo_Arg_Let      = "let"
	Mongo_Arg_Pipeline = "pipeline"
	Mongo_Arg_Branches = "branches"
	Mongo_Arg_Case     = "case"
	Mongo_Arg_Default  = "default"
	Mongo_Arg_Missing  = "missing"
)

var (
	Mongo_Binary_Operator_Mapping = map[opcode.Op]string{
		opcode.Plus:  Mongo_Operator_Add,
		opcode.Minus: Mongo_Operator_Subtract,
		opcode.Mul:   Mongo_Operator_Multiply,
		opcode.Div:   Mongo_Operator_Divide,
		opcode.GE:    Mongo_Operator_Gte,
		opcode.GT:    Mongo_Operator_Gt,
		opcode.LE:    Mongo_Operator_Lte,
		opcode.LT:    Mongo_Operator_Lt,
		opcode.EQ:    Mongo_Operator_Eq,
		opcode.NE:    Mongo_Operator_Ne,
		opcode.Like:  Mongo_Operator_Regex,
	}

	// graphQL字段名称正则表达式
	regex, _ = regexp.Compile(`^[_a-zA-Z][_a-zA-Z0-9]*$`)
)

// mongo查询语句转化器
type MongoQueryConverter struct {
	sql        parser.SQL
	query      *MongoQuery
	sourceView map[string]string
	validator  ConverterValidator
}

// mongo查询语句转化校验器
type MongoQueryConverterValidator struct{}

func (validator *MongoQueryConverterValidator) Validate(sql parser.SQL) (err error) {
	if sql == nil {
		err = fmt.Errorf("no sql to convert")
		return
	}
	if _, ok := sql.(*parser.SQLSelect); !ok {
		err = fmt.Errorf("MongoQueryConverterValidator can only validate SQLSelect,but got [%T]", sql)
		return
	}

	err = validateSelectField(sql.(*parser.SQLSelect))

	return
}

func validateSelectField(sql *parser.SQLSelect) (err error) {
	if sql.From.Ref != nil {
		err = validateSelectField(sql.From.Ref)
		if err != nil {
			return
		}
	}
	if sql.From.Left.Ref != nil {
		err = validateSelectField(sql.From.Left.Ref)
		if err != nil {
			return
		}
	}
	if sql.From.Right != nil && sql.From.Right.Ref != nil {
		err = validateSelectField(sql.From.Right.Ref)
		if err != nil {
			return
		}
	}
	for _, field := range sql.Fields {
		if field.GetType() == parser.SQLField_Type_WildCard {
			continue
		}
		// 查询结果字段名必须满足mongo和graphQL字段命名要求
		if !regex.MatchString(field.GetAsName()) {
			err = fmt.Errorf("invalid select column name [%v]", field.GetAsName())
			return
		}
	}

	return
}

type MongoQuery struct {
	Sql parser.SQL
	// 最终查询字段
	SelectFields []*parser.Column
	// 聚合起始view
	StartView string
	// 最终生成的view
	Pipeline bson.A
}

type MongoPipeline struct {
	ResultView string
	Pipeline   bson.A
}

func (query MongoQuery) OriginalSQL() parser.SQL {
	return query.Sql
}

func NewMongoQueryConverter(sql parser.SQL, sourceView map[string]string) SelectConverter {
	return &MongoQueryConverter{sql: sql, sourceView: sourceView, validator: &MongoQueryConverterValidator{}}
}

func (conv *MongoQueryConverter) Convert() (query Query, err error) {
	if conv.sql == nil {
		err = fmt.Errorf("no sql to convert")
		return
	}
	if _, ok := conv.sql.(*parser.SQLSelect); !ok {
		err = fmt.Errorf("MongoQueryConverter can only convert SQLSelect,but got [%T]", conv.sql)
		return
	}

	sql := conv.sql.(*parser.SQLSelect)

	err = conv.validator.Validate(sql)
	if err != nil {
		return
	}

	var startView string
	startView, err = getStartView(sql.From, conv.sourceView)
	if err != nil {
		return
	}

	selectFields := getSelectFieldsFromSql(sql)

	var pipeline bson.A
	pipeline, err = buildPipeline(sql, conv.sourceView)
	if err != nil {
		return
	}

	conv.query = &MongoQuery{
		Sql:          conv.sql,
		SelectFields: selectFields,
		StartView:    startView,
		Pipeline:     pipeline,
	}

	query = conv.query

	return
}

func getSelectFieldsFromSql(sql *parser.SQLSelect) (cols []*parser.Column) {
	var hasWildCard bool
	for _, field := range sql.Fields {
		if field.GetType() == parser.SQLField_Type_WildCard {
			hasWildCard = true
			continue
		}
		cols = append(cols, &parser.Column{Type: field.GetColType(), Name: field.GetAsName()})
	}

	if hasWildCard {
		cols = append(cols, getFieldsFromTable(sql.From)...)
	}

	return
}

func getFieldsFromTable(table *parser.SQLTable) (cols []*parser.Column) {
	if table == nil {
		return
	}

	if table.Ref != nil {
		cols = append(cols, getSelectFieldsFromSql(table.Ref)...)
		return
	}

	if table.Left != nil {
		cols = append(cols, getFieldsFromTable(table.Left)...)
	}
	if table.Right != nil {
		rightCols := map[string]*parser.Column{}
		for _, col := range getFieldsFromTable(table.Right) {
			rightCols[col.Name] = col
		}
		cols = append(cols, &parser.Column{
			Name: table.Right.GetAsName(),
			Type: parser.Json,
			Json: rightCols,
		})
	}

	if table.Name != nil {
		for _, vals := range maps.Values(table.TableSchema) {
			cols = append(cols, vals...)
		}
	}

	return
}

func getStartView(table *parser.SQLTable, sourceView map[string]string) (startView string, err error) {
	if table.Left != nil && table.Left.Ref != nil {
		startView, err = getStartView(table.Left.Ref.From, sourceView)
		return
	}
	if table.Left.Name == nil {
		startView, err = getStartView(table.Left, sourceView)
		return
	}
	if _, ok := sourceView[*table.Left.Name]; !ok {
		err = fmt.Errorf("can't find view with table %v", *table.Left.Name)
		return
	}
	startView = sourceView[*table.Left.Name]
	return
}

func buildPipeline(sql *parser.SQLSelect, sourceView map[string]string) (pipeline bson.A, err error) {
	if sql.From.Left != nil && sql.From.Left.Ref != nil {
		var subPipeline bson.A
		subPipeline, err = buildPipeline(sql.From.Left.Ref, sourceView)
		if err != nil {
			return
		}
		if len(subPipeline) > 0 {
			pipeline = append(pipeline, subPipeline...)
		}
	}

	if sql.From.Right != nil {
		let, match := getJoinOnField(sql.From.On, sql.From.Right)
		right := sql.From.Right
		if right.Ref != nil {
			var rightStartView string
			rightStartView, err = getStartView(right.Ref.From, sourceView)
			if err != nil {
				return
			}

			pipelineMatch := bson.M{
				Mongo_Stage_Macth: bson.M{
					Mongo_Operator_Expr: match,
				},
			}
			var nestedPipeline bson.A
			nestedPipeline, err = buildPipeline(right.Ref, sourceView)
			if err != nil {
				return
			}
			nestedPipeline = append(nestedPipeline, pipelineMatch)
			pipeline = append(pipeline,
				bson.M{
					Mongo_Stage_Lookup: bson.M{
						"from":             rightStartView,
						Mongo_Arg_Let:      let,
						Mongo_Arg_Pipeline: nestedPipeline,
						"as":               right.GetAsName(),
					},
				},
				bson.M{
					Mongo_Stage_Unwind: bson.M{
						"path":                       fmt.Sprintf("$%v", right.GetAsName()),
						"preserveNullAndEmptyArrays": true,
					},
				})
		} else {
			view := sourceView[*right.Name]
			if view == "" {
				err = fmt.Errorf("can't find view with name [%v]", right.GetAsName())
				return
			}
			pipelineMatch := bson.M{
				Mongo_Stage_Macth: bson.M{
					Mongo_Operator_Expr: match,
				},
			}
			// 需要执行连接查询
			pipeline = append(pipeline,
				bson.M{
					Mongo_Stage_Lookup: bson.M{
						"from":             view,
						Mongo_Arg_Let:      let,
						Mongo_Arg_Pipeline: bson.A{pipelineMatch},
						"as":               right.GetAsName(),
					},
				},
				bson.M{
					Mongo_Stage_Unwind: bson.M{
						"path":                       fmt.Sprintf("$%v", right.GetAsName()),
						"preserveNullAndEmptyArrays": true,
					},
				})
		}
	}

	if sql.Where != nil {
		pipeline = append(pipeline, bson.M{
			Mongo_Stage_Macth: bson.M{
				Mongo_Operator_Expr: parseFieldValue(sql.Where, sql.From.Right, false),
			},
		})
	}

	if len(sql.GroupBy) > 0 {
		groupBy := bson.M{}
		replace := bson.M{}
		for _, field := range sql.GroupBy {
			key := strings.ReplaceAll(parseFieldKey(field, sql.From.Right), ".", "_")
			groupBy[key] = parseFieldValue(field, sql.From.Right, false)
			replace[key] = fmt.Sprintf("$_id.%v", key)
		}
		group := bson.M{
			"_id": groupBy,
		}

		for _, field := range sql.Fields {
			if field.GetType() == parser.SQLField_Type_Normal || field.GetType() == parser.SQLField_Type_Value || field.GetColType() == parser.Json {
				key := strings.ReplaceAll(parseFieldKey(field, sql.From.Right), ".", "_")
				if val, ok := replace[key]; ok {
					delete(replace, key)
					// 查询分组字段可能有别名，需要重新替换
					replace[field.GetAsName()] = val
				}
				continue
			}
			if shouldSplitToTwoStep(field) {
				spiltToTwoStep(field, group, replace, sql.From.Right)
				continue
			}

			if _, ok := replace[field.GetAsName()]; ok {
				// 查询字段可能已存在于group by中，无需重复查询
				continue
			}

			group[field.GetAsName()] = parseFieldValue(field, sql.From.Right, false)
			if field.GetDistinct() && field.GetFunc() == parser.SQLFuncName_Count {
				replace[field.GetAsName()] = bson.M{Mongo_Operator_Size: fmt.Sprintf("$%v", field.GetAsName())}
			} else {
				replace[field.GetAsName()] = fmt.Sprintf("$%v", field.GetAsName())
			}
		}

		pipeline = append(pipeline,
			bson.M{Mongo_Stage_Group: group},
			bson.M{Mongo_Stage_Replace_Root: bson.M{
				"newRoot": replace,
			}},
		)

		if sql.Having != nil {
			pipeline = append(pipeline, bson.M{
				Mongo_Stage_Macth: bson.M{
					Mongo_Operator_Expr: parseFieldValue(sql.Having.(*parser.BinaryOperation), sql.From.Right, false),
				},
			})
		}
	} else {
		var hasWildCard bool
		aggFunc := sql.Distinct
		for _, field := range sql.Fields {
			if field.GetType() == parser.SQLField_Type_WildCard {
				hasWildCard = true
			}
			if field.GetType() == parser.SQLField_Type_Agg_Func || field.HasDistinct() {
				aggFunc = true
			}
		}
		if hasWildCard {
			if len(sql.Fields) > 1 {
				// 还有其他字段需要输出
				addFields := bson.M{}
				for _, field := range sql.Fields {
					if field.GetType() == parser.SQLField_Type_WildCard {
						continue
					}
					addFields[field.GetAsName()] = parseFieldValue(field, sql.From.Right, false)
				}
				if len(addFields) > 0 {
					pipeline = append(pipeline, bson.M{
						Mongo_Stage_AddFields: addFields,
					})
				}
			}
		} else {
			if aggFunc {
				// 输出聚合结果
				group := bson.M{
					"_id": nil,
				}
				replace := bson.M{}
				for _, field := range sql.Fields {
					if shouldSplitToTwoStep(field) {
						spiltToTwoStep(field, group, replace, sql.From.Right)
						continue
					}
					group[field.GetAsName()] = parseFieldValue(field, sql.From.Right, false)
					if field.GetDistinct() && field.GetFunc() == parser.SQLFuncName_Count {
						replace[field.GetAsName()] = bson.M{Mongo_Operator_Size: fmt.Sprintf("$%v", field.GetAsName())}
					} else {
						replace[field.GetAsName()] = fmt.Sprintf("$%v", field.GetAsName())
					}
				}
				pipeline = append(pipeline,
					bson.M{Mongo_Stage_Group: group},
					bson.M{Mongo_Stage_Replace_Root: bson.M{
						"newRoot": replace,
					}},
				)
			} else {
				// 只输出指定字段
				replace := bson.M{}
				for _, field := range sql.Fields {
					replace[field.GetAsName()] = parseFieldValue(field, sql.From.Right, true)
				}
				pipeline = append(pipeline, bson.M{
					Mongo_Stage_Replace_Root: bson.M{
						"newRoot": replace,
					},
				})
			}
		}
	}

	if len(sql.OrderBy) > 0 {
		sort := bson.M{}
		for _, order := range sql.OrderBy {
			if order.Desc {
				sort[order.GetAsName()] = -1
			} else {
				sort[order.GetAsName()] = 1
			}
		}
		pipeline = append(pipeline, bson.M{
			Mongo_Stage_Sort: sort,
		})
	}

	if sql.Limit != nil {
		if sql.Limit.Offset != nil {
			pipeline = append(pipeline, bson.M{
				Mongo_Stage_Skip: *sql.Limit.Offset,
			})
		}
		if sql.Limit.Limit != nil {
			pipeline = append(pipeline, bson.M{
				Mongo_Stage_Limit: *sql.Limit.Limit,
			})
		}
	}

	return
}

// 1)对聚合函数使用取值函数
// 2)聚合函数带去重逻辑，需要先获取各参数的值，然后在replace中执行函数，因为去重需要两步
func shouldSplitToTwoStep(field parser.ExprField) bool {
	if field.GetType() == parser.SQLField_Type_Func {
		return true
	}
	if field.GetType() == parser.SQLField_Type_Agg_Func && field.HasDistinct() {
		return true
	}
	if bo, ok := field.(*parser.BinaryOperation); ok {
		if slices.Contains([]opcode.Op{opcode.Plus, opcode.Minus, opcode.Mul, opcode.Div}, *bo.Operator) {
			return true
		}
	}
	return false
}

func spiltToTwoStep(field parser.ExprField, group bson.M, replace bson.M, right *parser.SQLTable) {
	// 函数带去重逻辑，需要先获取各参数的值，然后在replace中执行函数，因为去重需要两步
	replaceField := buildSplitReplaceField(field, group, right, "")
	replace[field.GetAsName()] = parseFieldValue(replaceField, right, true)
}

func buildSplitReplaceField(field parser.ExprField, group bson.M, right *parser.SQLTable, argPrefix string) (replaceField parser.ExprField) {
	var args []parser.ExprField
	for i := 0; i < len(field.GetArgs()); i++ {
		funArg := field.GetArgs()[i]
		var argName string
		if argPrefix == "" {
			argName = fmt.Sprintf("%v_%v", field.GetAsName(), i)
		} else {
			argName = fmt.Sprintf("%v_%v", argPrefix, i)
		}

		if shouldSplitToTwoStep(funArg) {
			// 需要拆分的函数递归求解
			args = append(args, buildSplitReplaceField(funArg, group, right, argName))
		} else if funArg.IsAllValue() {
			// 常量无需group，直接放到最后计算
			args = append(args, funArg)
		} else {
			// 无需拆分的函数直接求值
			group[argName] = parseFieldValue(funArg, right, false)
			args = append(args, &parser.SQLField{
				Type: parser.SQLField_Type_Normal,
				Name: &parser.Column{
					Name: argName,
				},
			})
		}
	}

	if bo, ok := field.(*parser.BinaryOperation); ok {
		replaceField = &parser.BinaryOperation{
			Operator: bo.Operator,
			Left:     args[0],
			Right:    args[1],
		}
	} else {
		fn := field.GetFunc()
		replaceField = &parser.SQLField{
			Type:     field.GetType(),
			Func:     &fn,
			Name:     field.GetColName(),
			Distinct: field.GetDistinct(),
			Args:     args,
		}
	}

	return
}

func getJoinOnField(joinOn parser.ExprField, rightTable *parser.SQLTable) (let bson.M, match bson.M) {
	let = bson.M{}
	match = bson.M{}
	var bo *parser.BinaryOperation
	var ok bool
	if bo, ok = joinOn.(*parser.BinaryOperation); !ok {
		return
	}
	if *bo.Operator == opcode.LogicAnd {
		leftLet, leftMatch := getJoinOnField(bo.Left, rightTable)
		rightLet, rightMatch := getJoinOnField(bo.Right, rightTable)
		for k, v := range leftLet {
			let[k] = v
		}
		for k, v := range rightLet {
			let[k] = v
		}
		match = bson.M{Mongo_Operator_And: bson.A{leftMatch, rightMatch}}
		return
	}
	if *bo.Operator == opcode.LogicOr {
		leftLet, leftMatch := getJoinOnField(bo.Left, rightTable)
		rightLet, rightMatch := getJoinOnField(bo.Right, rightTable)
		for k, v := range leftLet {
			let[k] = v
		}
		for k, v := range rightLet {
			let[k] = v
		}
		match = bson.M{Mongo_Operator_Or: bson.A{leftMatch, rightMatch}}
		return
	}

	leftVal := parseFieldValue(bo.Left, nil, true)
	rightVal := parseFieldValue(bo.Right, nil, true)
	if _, ok := rightTable.TableSchema[bo.Right.GetTable()]; ok {
		// 左边字段在左表，右边字段在右表
		if bo.Left.GetType() == parser.SQLField_Type_Value {
			if val, ok := leftVal.(int64); ok && bo.Right.GetColType() == parser.Boolean {
				match[Mongo_Operator_Or] = bson.A{
					bson.M{Mongo_Binary_Operator_Mapping[*bo.Operator]: bson.A{leftVal, rightVal}},
					bson.M{Mongo_Binary_Operator_Mapping[*bo.Operator]: bson.A{val > 0, rightVal}},
				}
			} else {
				match[Mongo_Binary_Operator_Mapping[*bo.Operator]] = bson.A{leftVal, rightVal}
			}
		} else {
			letKey := strings.ToLower(bo.Left.GetRawName())
			let[letKey] = leftVal
			match[Mongo_Binary_Operator_Mapping[*bo.Operator]] = bson.A{fmt.Sprintf("$$%v", letKey), rightVal}
		}

	} else {
		// 左边字段在右表，右边字段在左表
		if bo.Right.GetType() == parser.SQLField_Type_Value {
			if val, ok := rightVal.(int64); ok && bo.Left.GetColType() == parser.Boolean {
				match[Mongo_Operator_Or] = bson.A{
					bson.M{Mongo_Binary_Operator_Mapping[*bo.Operator]: bson.A{rightVal, leftVal}},
					bson.M{Mongo_Binary_Operator_Mapping[*bo.Operator]: bson.A{val > 0, leftVal}},
				}
			} else {
				match[Mongo_Binary_Operator_Mapping[*bo.Operator]] = bson.A{leftVal, rightVal}
			}
		} else {
			letKey := strings.ToLower(bo.Right.GetRawName())
			let[letKey] = rightVal
			match[Mongo_Binary_Operator_Mapping[*bo.Operator]] = bson.A{fmt.Sprintf("$$%v", letKey), leftVal}
		}
	}

	return
}

func convertBinaryOperation(field parser.ExprField, rightTable *parser.SQLTable, isReplace bool) (match bson.M) {
	var bo *parser.BinaryOperation
	var ok bool
	if bo, ok = field.(*parser.BinaryOperation); !ok {
		return
	}
	if *bo.Operator == opcode.LogicAnd {
		left := convertBinaryOperation(bo.Left, rightTable, isReplace)
		right := convertBinaryOperation(bo.Right, rightTable, isReplace)
		match = bson.M{Mongo_Operator_And: bson.A{left, right}}
		return
	}
	if *bo.Operator == opcode.LogicOr {
		left := convertBinaryOperation(bo.Left, rightTable, isReplace)
		right := convertBinaryOperation(bo.Right, rightTable, isReplace)
		match = bson.M{Mongo_Operator_Or: bson.A{left, right}}
		return
	}
	if *bo.Operator == opcode.In {
		left := parseFieldValue(bo.Left, rightTable, isReplace)
		var right bson.A
		for _, arg := range bo.Right.GetArgs() {
			right = append(right, parseFieldValue(arg, rightTable, isReplace))
		}
		match = bson.M{Mongo_Operator_In: bson.A{left, right}}
		if bo.Not {
			match = bson.M{Mongo_Operator_Not: match}
		}
		return
	}

	leftVal := parseFieldValue(bo.Left, rightTable, isReplace)
	rightVal := parseFieldValue(bo.Right, rightTable, isReplace)
	if *bo.Operator == opcode.Like {
		rightVal = fmt.Sprintf(".*%v.*", rightVal)
	}
	if bo.Left.GetColType() == parser.Datetime {
		// 解析为时间
		timeVal, err := ParseTime("", fmt.Sprint(rightVal))
		if err != nil {
			log.Warnf("invalid datetime value=%v", rightVal)
		} else {
			rightVal = timeVal
		}
	}

	if *bo.Operator == opcode.EQ {
		if rightVal == nil {
			// 字段不存在也认为是空
			match = bson.M{
				Mongo_Operator_Or: bson.A{
					bson.M{Mongo_Operator_Eq: bson.A{leftVal, rightVal}},
					bson.M{Mongo_Operator_Eq: bson.A{bson.M{Mongo_Operator_Type: leftVal}, Mongo_Arg_Missing}},
				},
			}
			return
		}

		if intVal, ok := rightVal.(int64); ok {
			if intVal == 0 || intVal == 1 {
				// 可能是匹配布尔值
				match = bson.M{
					Mongo_Operator_Or: bson.A{
						bson.M{Mongo_Operator_Eq: bson.A{leftVal, rightVal}},
						bson.M{Mongo_Operator_Eq: bson.A{leftVal, intVal == 1}},
					},
				}
				return
			}
		}
	}

	if *bo.Operator == opcode.NE && rightVal == nil {
		// 字段不存在也认为是空
		match = bson.M{
			Mongo_Operator_And: bson.A{
				bson.M{Mongo_Operator_Ne: bson.A{leftVal, rightVal}},
				bson.M{Mongo_Operator_Ne: bson.A{bson.M{Mongo_Operator_Type: leftVal}, Mongo_Arg_Missing}},
			},
		}
		return
	}

	match = bson.M{Mongo_Binary_Operator_Mapping[*bo.Operator]: bson.A{leftVal, rightVal}}

	return
}

func parseFieldValue(field parser.ExprField, right *parser.SQLTable, isReplace bool) (val any) {
	if bo, ok := field.(*parser.BinaryOperation); ok {
		// 二元表达式单独处理
		val = convertBinaryOperation(bo, right, isReplace)
		return
	}

	switch field.GetType() {
	case parser.SQLField_Type_Normal:
		if field.GetDistinct() {
			val = bson.M{
				Mongo_Operator_AddToSet: fmt.Sprintf("$%v", parseFieldKey(field, right)),
			}
		} else {
			val = fmt.Sprintf("$%v", parseFieldKey(field, right))
		}
	case parser.SQLField_Type_Func:
		switch field.GetFunc() {
		case parser.SQLFuncName_Json_Extract:
			val = fmt.Sprintf("$%v", parseFieldKey(field, right))
		case parser.SQLFuncName_Year:
			val = bson.M{
				Mongo_Operator_Year: parseFieldValue(field.GetArgs()[0], right, isReplace),
			}
		case parser.SQLFuncName_Month:
			val = bson.M{
				Mongo_Operator_Month: parseFieldValue(field.GetArgs()[0], right, isReplace),
			}
		case parser.SQLFuncName_Day:
			val = bson.M{
				Mongo_Operator_DayOfMonth: parseFieldValue(field.GetArgs()[0], right, isReplace),
			}
		case parser.SQLFuncName_Hour:
			val = bson.M{
				Mongo_Operator_Hour: parseFieldValue(field.GetArgs()[0], right, isReplace),
			}
		case parser.SQLFuncName_Minute:
			val = bson.M{
				Mongo_Operator_Minute: parseFieldValue(field.GetArgs()[0], right, isReplace),
			}
		case parser.SQLFuncName_Second:
			val = bson.M{
				Mongo_Operator_Second: parseFieldValue(field.GetArgs()[0], right, isReplace),
			}
		case parser.SQLFuncName_Date_Format:
			val = bson.M{
				Mongo_Operator_DateToString: bson.M{
					"date":   parseFieldValue(field.GetArgs()[0], right, isReplace),
					"format": parseFieldValue(field.GetArgs()[1], right, isReplace),
				},
			}
		case parser.SQLFuncName_Round:
			val = bson.M{
				Mongo_Operator_Round: bson.A{
					parseFieldValue(field.GetArgs()[0], right, isReplace),
					parseFieldValue(field.GetArgs()[1], right, isReplace),
				},
			}
		case parser.SQLFuncName_If:
			val = bson.M{
				Mongo_Operator_Cond: bson.M{
					Mongo_Arg_If:   parseFieldValue(field.GetArgs()[0], right, isReplace),
					Mongo_Arg_Then: parseFieldValue(field.GetArgs()[1], right, isReplace),
					Mongo_Arg_Else: parseFieldValue(field.GetArgs()[2], right, isReplace),
				},
			}
		case parser.SQLFuncName_Case:
			caseWhen := field.(*parser.CaseWhenExpr)
			var branches bson.A
			for _, whenClause := range caseWhen.WhenClauses {
				branches = append(branches, bson.M{
					Mongo_Arg_Case: parseFieldValue(whenClause.Expr, right, isReplace),
					Mongo_Arg_Then: parseFieldValue(whenClause.Result, right, isReplace),
				})
			}
			mongoSwitch := bson.M{Mongo_Arg_Branches: branches}
			if caseWhen.Else != nil {
				mongoSwitch[Mongo_Arg_Default] = parseFieldValue(caseWhen.Else, right, isReplace)
			}
			val = bson.M{
				Mongo_Operator_Switch: mongoSwitch,
			}
		case parser.SQLFuncName_Now:
			val = Mongo_Operator_Now
		case parser.SQLFuncName_To_Double:
			val = parseFieldValue(field.GetArgs()[0], right, isReplace)
			val = bson.M{Mongo_Operator_To_Double: val}
		case parser.SQLFuncName_To_Floor:
			val = parseFieldValue(field.GetArgs()[0], right, isReplace)
			val = bson.M{Mongo_Operator_Floor: val}
		case parser.SQLFuncName_Substring_Index:
			arg1 := parseFieldValue(field.GetArgs()[0], right, isReplace)
			arg2 := parseFieldValue(field.GetArgs()[1], right, isReplace)
			arg3 := parseFieldValue(field.GetArgs()[2], right, isReplace)
			val = bson.M{Mongo_Operator_ArrayElemAt: bson.A{
				bson.M{Mongo_Operator_Split: bson.A{arg1, arg2}},
				arg3,
			}}
		}
	case parser.SQLField_Type_Agg_Func:
		switch field.GetFunc() {
		case parser.SQLFuncName_Sum, parser.SQLFuncName_Avg, parser.SQLFuncName_Max, parser.SQLFuncName_Min:
			if field.GetDistinct() {
				val = parseFieldValue(field.GetArgs()[0], right, isReplace)
				if isReplace {
					val = bson.M{fmt.Sprintf("$%v", strings.ToLower(field.GetFunc())): val}
				}
			} else {
				val = bson.M{
					fmt.Sprintf("$%v", strings.ToLower(field.GetFunc())): parseFieldValue(field.GetArgs()[0], right, isReplace),
				}
			}
		case parser.SQLFuncName_Count:
			if field.GetDistinct() {
				val = parseFieldValue(field.GetArgs()[0], right, isReplace)
				if isReplace {
					// count常量结果为1
					if field.GetArgs()[0].GetType() == parser.SQLField_Type_Value {
						val = 1
					} else {
						val = bson.M{Mongo_Operator_Size: val}
					}
				}
			} else {
				arg := field.GetArgs()[0]
				if arg.GetType() == parser.SQLField_Type_Normal {
					// 参数为具体字段则需要根据具体字段是否存在来统计
					val = bson.M{
						Mongo_Operator_Sum: bson.M{
							Mongo_Operator_Cond: bson.M{
								Mongo_Arg_If: bson.M{
									Mongo_Operator_IfNull: bson.A{
										parseFieldValue(arg, right, isReplace),
										false,
									},
								},
								Mongo_Arg_Then: 1,
								Mongo_Arg_Else: 0,
							},
						},
					}
				} else {
					// count(1)直接统计总数
					val = bson.M{
						Mongo_Operator_Sum: 1,
					}
				}
			}
		case parser.SQLFuncName_Json_Arr_Agg:
			val = bson.M{Mongo_Operator_Push: parseFieldValue(field.GetArgs()[0], right, isReplace)}
		}
	case parser.SQLField_Type_Value:
		val = field.GetValue()
	}

	return
}

func parseFieldKey(field parser.ExprField, right *parser.SQLTable) (key string) {
	switch field.GetType() {
	case parser.SQLField_Type_Normal:
		key = field.GetRawName()
	case parser.SQLField_Type_Func:
		if field.GetFunc() == parser.SQLFuncName_Json_Extract {
			col := field.GetArgs()[0]
			val := field.GetArgs()[1]
			path := fmt.Sprint(val.GetValue())[2:]
			key = fmt.Sprintf("%v.%v", col.GetRawName(), path)
		} else {
			key = field.GetAsName()
		}
	case parser.SQLField_Type_Value:
		key = fmt.Sprint(field.GetValue())
	}

	if field.GetTable() != "" && right != nil && field.GetTable() == right.GetAsName() {
		key = fmt.Sprintf("%v.%v", field.GetTable(), key)
	}

	return
}

// 解析时间
// layout 时间格式，未指定则默认使用RFC3339
// 时间字符串，长度为10说明只包含日期，使用DateOnly格式
func ParseTime(layout, value string) (timeVal time.Time, err error) {
	if layout == "" {
		layout = time.RFC3339
	}

	if len(value) == 10 {
		layout = time.DateOnly
	}

	timeVal, err = time.Parse(layout, value)

	return
}
