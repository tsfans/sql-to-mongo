package converter

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/parser/opcode"
	log "github.com/sirupsen/logrus"
	"github.com/tsfans/sql_to_mongo/parser"
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

	Mongo_Arg_If       = "if"
	Mongo_Arg_Then     = "then"
	Mongo_Arg_Else     = "else"
	Mongo_Arg_Let      = "let"
	Mongo_Arg_Pipeline = "pipeline"
)

var (
	Mongo_Binary_Operator_Mapping = map[opcode.Op]string{
		opcode.GE:   Mongo_Operator_Gte,
		opcode.GT:   Mongo_Operator_Gt,
		opcode.LE:   Mongo_Operator_Lte,
		opcode.LT:   Mongo_Operator_Lt,
		opcode.EQ:   Mongo_Operator_Eq,
		opcode.NE:   Mongo_Operator_Ne,
		opcode.Like: Mongo_Operator_Regex,
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
		if field.Type == parser.SQLField_Type_WildCard {
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
	startView, err = getStartView(sql, conv.sourceView)
	if err != nil {
		return
	}

	selectFields := getSelectFieldsFromSql(sql)

	var pipeline bson.A
	pipeline, err = buildPipeline(sql, conv.sourceView)
	if err != nil {
		return
	}

	log.Debugf("startView=%v", startView)
	sf, _ := json.Marshal(selectFields)
	log.Debugf("selectFields=%v", string(sf))
	p, _ := json.Marshal(pipeline)
	log.Debugf("pipeline=%v", string(p))

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
		if field.Type == parser.SQLField_Type_WildCard {
			hasWildCard = true
			continue
		}
		cols = append(cols, &parser.Column{Type: field.Name.Type, Name: field.GetAsName()})
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

func getStartView(sql *parser.SQLSelect, sourceView map[string]string) (startView string, err error) {
	if sql.From.Left != nil && sql.From.Left.Ref != nil {
		startView, err = getStartView(sql.From.Left.Ref, sourceView)
		return
	}
	if _, ok := sourceView[*sql.From.Left.Name]; !ok {
		err = fmt.Errorf("can't find view with table %v", *sql.From.Left.Name)
		return
	}
	startView = sourceView[*sql.From.Left.Name]
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
		var leftField *parser.Column
		var rightField *parser.Column
		leftField, rightField, err = getJoinOnField(sql.From)
		if err != nil {
			return
		}
		right := sql.From.Right
		if right.Ref != nil {
			var rightStartView string
			rightStartView, err = getStartView(right.Ref, sourceView)
			if err != nil {
				return
			}

			// let参数不能以大写字母开头，因此需要转换一下
			letKey := strings.ToLower(leftField.Name)
			let := bson.M{
				letKey: fmt.Sprintf("$%v", leftField.Name),
			}
			match := bson.M{
				Mongo_Stage_Macth: bson.M{
					Mongo_Operator_Expr: bson.M{
						Mongo_Operator_Eq: bson.A{
							fmt.Sprintf("$$%v", letKey),
							fmt.Sprintf("$%v", rightField.Name),
						},
					},
				},
			}
			var nestedPipeline bson.A
			nestedPipeline, err = buildPipeline(right.Ref, sourceView)
			if err != nil {
				return
			}
			nestedPipeline = append(nestedPipeline, match)
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
			// 需要执行连接查询
			pipeline = append(pipeline,
				bson.M{
					Mongo_Stage_Lookup: bson.M{
						"from":         view,
						"localField":   leftField.Name,
						"foreignField": rightField.Name,
						"as":           right.GetAsName(),
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
			Mongo_Stage_Macth: convertBinaryOperation(sql.Where, sql.From.Right),
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
			if field.Type == parser.SQLField_Type_Normal || field.Type == parser.SQLField_Type_Value || field.Name.Type == parser.Json {
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
			if field.Distinct && field.Func != nil && *field.Func == parser.SQLFuncName_Count {
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
				Mongo_Stage_Macth: convertBinaryOperation(sql.Having, sql.From.Right),
			})
		}
	} else {
		var hasWildCard bool
		aggFunc := sql.Distinct
		for _, field := range sql.Fields {
			if field.Type == parser.SQLField_Type_WildCard {
				hasWildCard = true
			}
			if field.Type == parser.SQLField_Type_Agg_Func || field.HasDistinct() {
				aggFunc = true
			}
		}
		if hasWildCard {
			if len(sql.Fields) > 1 {
				// 还有其他字段需要输出
				addFields := bson.M{}
				for _, field := range sql.Fields {
					if field.Type == parser.SQLField_Type_WildCard {
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
					if field.Distinct && field.Func != nil && *field.Func == parser.SQLFuncName_Count {
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

// // 函数带去重逻辑，需要先获取各参数的值，然后在replace中执行函数，因为去重需要两步
func shouldSplitToTwoStep(field *parser.SQLField) bool {
	return (field.Type == parser.SQLField_Type_Agg_Func || field.Type == parser.SQLField_Type_Func) && field.HasDistinct()
}

func spiltToTwoStep(field *parser.SQLField, group bson.M, replace bson.M, right *parser.SQLTable) {
	// 函数带去重逻辑，需要先获取各参数的值，然后在replace中执行函数，因为去重需要两步
	replaceField := buildSplitReplaceField(field, group, right, "")
	replace[field.GetAsName()] = parseFieldValue(replaceField, right, true)
}

func buildSplitReplaceField(field *parser.SQLField, group bson.M, right *parser.SQLTable, argPrefix string) (replaceField *parser.SQLField) {
	var args []*parser.SQLField
	for i := 0; i < len(field.Args); i++ {
		funArg := field.Args[i]
		var argName string
		if argPrefix == "" {
			argName = fmt.Sprintf("%v_%v", field.GetAsName(), i)
		} else {
			argName = fmt.Sprintf("%v_%v", argPrefix, i)
		}

		if funArg.HasDistinct() {
			// 将distinct对象addToSet
			if funArg.Distinct && (funArg.Type == parser.SQLField_Type_Normal || funArg.Type == parser.SQLField_Type_Value) {
				group[argName] = parseFieldValue(funArg, right, false)
				args = append(args, &parser.SQLField{
					Type: parser.SQLField_Type_Normal,
					Name: &parser.Column{
						Name: argName,
					},
				})
				continue
			}
			// 其他函数递归求解
			args = append(args, buildSplitReplaceField(funArg, group, right, argName))
		} else {
			// 常量无需group，直接放到最后计算
			if funArg.IsAllValue() {
				args = append(args, funArg)
				continue
			}
			// 不带distinct的函数直接求值
			group[argName] = parseFieldValue(funArg, right, false)
			args = append(args, &parser.SQLField{
				Type: parser.SQLField_Type_Normal,
				Name: &parser.Column{
					Name: argName,
				},
			})
		}
	}

	replaceField = &parser.SQLField{
		Type:     field.Type,
		Func:     field.Func,
		Name:     field.Name,
		Distinct: field.Distinct,
		Args:     args,
	}

	return
}

func getJoinOnField(table *parser.SQLTable) (leftField *parser.Column, rightField *parser.Column, err error) {
	joinOn := table.On
	for tableName, cols := range table.Left.TableSchema {
		for _, col := range cols {
			if joinOn.Left.Table != nil {
				if *joinOn.Left.Table == tableName && joinOn.Left.Name.Name == col.Name {
					leftField = col
					break
				}
			} else {
				if joinOn.Left.Name.Name == col.Name {
					leftField = col
					break
				}
			}
			if joinOn.Right.Table != nil {
				if *joinOn.Right.Table == tableName && joinOn.Right.Name.Name == col.Name {
					leftField = col
					break
				}
			} else {
				if joinOn.Right.Name.Name == col.Name {
					leftField = col
					break
				}
			}
		}
	}

	// 表连接条件必须是左右表字段，两个字段都在一边则无效
	if (leftField == nil && rightField == nil) || (leftField != nil && rightField != nil) {
		err = fmt.Errorf("invalid join on condition")
		return
	}

	for tableName, cols := range table.Right.TableSchema {
		for _, col := range cols {
			if joinOn.Left.Table != nil {
				if *joinOn.Left.Table == tableName && joinOn.Left.Name.Name == col.Name {
					rightField = col
					break
				}
			} else {
				if joinOn.Left.Name.Name == col.Name {
					rightField = col
					break
				}
			}
			if joinOn.Right.Table != nil {
				if *joinOn.Right.Table == tableName && joinOn.Right.Name.Name == col.Name {
					rightField = col
					break
				}
			} else {
				if joinOn.Right.Name.Name == col.Name {
					rightField = col
					break
				}
			}
		}
	}

	return
}

func convertBinaryOperation(bo *parser.BinaryOperation, right *parser.SQLTable) (match bson.M) {
	if *bo.Operator == opcode.LogicAnd {
		left := convertBinaryOperation(bo.Left, right)
		right := convertBinaryOperation(bo.Right, right)
		match = bson.M{
			Mongo_Operator_Expr: bson.M{
				Mongo_Operator_And: bson.A{
					left[Mongo_Operator_Expr],
					right[Mongo_Operator_Expr],
				},
			},
		}
		return
	}
	if *bo.Operator == opcode.LogicOr {
		left := convertBinaryOperation(bo.Left, right)
		right := convertBinaryOperation(bo.Right, right)
		match = bson.M{
			Mongo_Operator_Expr: bson.M{
				Mongo_Operator_Or: bson.A{
					left[Mongo_Operator_Expr],
					right[Mongo_Operator_Expr],
				},
			},
		}
		return
	}

	leftVal := parseFieldValue(&bo.Left.SQLField, right, false)
	rightVal := parseFieldValue(&bo.Right.SQLField, right, false)
	if *bo.Operator == opcode.Like {
		rightVal = fmt.Sprintf(".*%v.*", rightVal)
	}
	match = bson.M{
		Mongo_Operator_Expr: bson.M{
			Mongo_Binary_Operator_Mapping[*bo.Operator]: bson.A{
				leftVal,
				rightVal,
			},
		},
	}

	return
}

func parseFieldValue(field *parser.SQLField, right *parser.SQLTable, isReplace bool) (val any) {
	switch field.Type {
	case parser.SQLField_Type_Normal:
		if field.Distinct {
			val = bson.M{
				Mongo_Operator_AddToSet: fmt.Sprintf("$%v", parseFieldKey(field, right)),
			}
		} else {
			val = fmt.Sprintf("$%v", parseFieldKey(field, right))
		}
	case parser.SQLField_Type_Func:
		switch *field.Func {
		case parser.SQLFuncName_Json_Extract:
			val = fmt.Sprintf("$%v", parseFieldKey(field, right))
		case parser.SQLFuncName_Year:
			val = bson.M{
				Mongo_Operator_Year: parseFieldValue(field.Args[0], right, isReplace),
			}
		case parser.SQLFuncName_Month:
			val = bson.M{
				Mongo_Operator_Month: parseFieldValue(field.Args[0], right, isReplace),
			}
		case parser.SQLFuncName_Day:
			val = bson.M{
				Mongo_Operator_DayOfMonth: parseFieldValue(field.Args[0], right, isReplace),
			}
		case parser.SQLFuncName_Hour:
			val = bson.M{
				Mongo_Operator_Hour: parseFieldValue(field.Args[0], right, isReplace),
			}
		case parser.SQLFuncName_Minute:
			val = bson.M{
				Mongo_Operator_Minute: parseFieldValue(field.Args[0], right, isReplace),
			}
		case parser.SQLFuncName_Second:
			val = bson.M{
				Mongo_Operator_Second: parseFieldValue(field.Args[0], right, isReplace),
			}
		case parser.SQLFuncName_Date_Format:
			val = bson.M{
				Mongo_Operator_DateToString: bson.M{
					"date":   parseFieldValue(field.Args[0], right, isReplace),
					"format": parseFieldValue(field.Args[1], right, isReplace),
				},
			}
		case parser.SQLFuncName_Plus:
			val = bson.M{
				Mongo_Operator_Add: bson.A{
					parseFieldValue(field.Args[0], right, isReplace),
					parseFieldValue(field.Args[1], right, isReplace),
				},
			}
		case parser.SQLFuncName_Minus:
			val = bson.M{
				Mongo_Operator_Subtract: bson.A{
					parseFieldValue(field.Args[0], right, isReplace),
					parseFieldValue(field.Args[1], right, isReplace),
				},
			}
		case parser.SQLFuncName_Mul:
			val = bson.M{
				Mongo_Operator_Multiply: bson.A{
					parseFieldValue(field.Args[0], right, isReplace),
					parseFieldValue(field.Args[1], right, isReplace),
				},
			}
		case parser.SQLFuncName_Div:
			val = bson.M{
				Mongo_Operator_Divide: bson.A{
					parseFieldValue(field.Args[0], right, isReplace),
					parseFieldValue(field.Args[1], right, isReplace),
				},
			}
		case parser.SQLFuncName_Round:
			val = bson.M{
				Mongo_Operator_Round: bson.A{
					parseFieldValue(field.Args[0], right, isReplace),
					parseFieldValue(field.Args[1], right, isReplace),
				},
			}
		}
	case parser.SQLField_Type_Agg_Func:
		switch *field.Func {
		case parser.SQLFuncName_Sum, parser.SQLFuncName_Avg, parser.SQLFuncName_Max, parser.SQLFuncName_Min:
			if field.Distinct {
				val = parseFieldValue(field.Args[0], right, isReplace)
				if isReplace {
					val = bson.M{fmt.Sprintf("$%v", strings.ToLower(*field.Func)): val}
				}
			} else {
				val = bson.M{
					fmt.Sprintf("$%v", strings.ToLower(*field.Func)): parseFieldValue(field.Args[0], right, isReplace),
				}
			}
		case parser.SQLFuncName_Count:
			if field.Distinct {
				val = parseFieldValue(field.Args[0], right, isReplace)
				if isReplace {
					// count常量结果为1
					if field.Args[0].Type == parser.SQLField_Type_Value {
						val = 1
					} else {
						val = bson.M{Mongo_Operator_Size: val}
					}
				}
			} else {
				arg := field.Args[0]
				if arg.Type == parser.SQLField_Type_Normal {
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
		}
	case parser.SQLField_Type_Value:
		val = field.Value
	}

	return
}

func parseFieldKey(field *parser.SQLField, right *parser.SQLTable) (key string) {
	switch field.Type {
	case parser.SQLField_Type_Normal:
		key = field.Name.Name
	case parser.SQLField_Type_Func:
		if *field.Func == parser.SQLFuncName_Json_Extract {
			col := field.Args[0]
			val := field.Args[1]
			path := fmt.Sprint(val.Value)[2:]
			key = fmt.Sprintf("%v.%v", col.Name.Name, path)
		} else {
			key = field.GetAsName()
		}
	case parser.SQLField_Type_Value:
		key = fmt.Sprint(field.Value)
	}

	if field.Table != nil && right != nil && *field.Table == right.GetAsName() {
		key = fmt.Sprintf("%v.%v", *field.Table, key)
	}

	return
}
