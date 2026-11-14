// 值归一化:把各驱动返回的 Go 原始值转成可稳定序列化为 JSON 的形状
// MySQL 的 text、decimal 到 Go 侧都是 []byte, 金仓与 PostgreSQL 的 numeric 是结构体, 这里统一收口
package driver

import (
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
)

// timeLayout 时间统一输出为可读的年月日时分秒,与金仓、PostgreSQL 侧的文本格式一致
const timeLayout = "2006-01-02 15:04:05"

// Normalize 归一化单个列值
func Normalize(v any) any {

	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case []byte:
		// 文本列在 MySQL 驱动下也是字节,合法 UTF-8 按字符串输出,否则按 Base64
		if utf8.Valid(x) {
			return string(x)
		}
		return base64.StdEncoding.EncodeToString(x)
	case string:
		return x
	case [16]byte:
		// pgx 把 uuid 解码成 16 字节定长数组, 按标准 8-4-4-4-12 文本输出
		return formatUUID(x)
	case map[string]any, []any:
		// json 与 jsonb 解码后是 Go 的 map 与切片, 还原成 JSON 文本
		if raw, err := json.Marshal(x); err == nil {
			return string(raw)
		}
		return fmt.Sprintf("%v", v)
	case time.Time:
		// 统一输出完整时间, 不按零点短路成日期: datetime 列的零点值不能丢时分秒
		return x.Format(timeLayout)
	case bool, int64, json.Number:
		return x
	case float64:
		// PostgreSQL 的 float8 可以存非有限值, JSON 序列化会直接报错, 统一转成文本
		if s, ok := nonFiniteFloat(x); ok {
			return s
		}
		return x
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case uint:
		return uint64(x)
	case uint8:
		return uint64(x)
	case uint16:
		return uint64(x)
	case uint32:
		return uint64(x)
	case uint64:
		return x
	case float32:
		if s, ok := nonFiniteFloat(float64(x)); ok {
			return s
		}
		return float64(x)
	case pgtype.Numeric:
		// 精确十进制按文本输出,保持精度不失真
		if !x.Valid {
			return nil
		}
		if dv, err := x.Value(); err == nil {
			if s, ok := dv.(string); ok {
				// NaN 与 Infinity 是 numeric 的合法文本, 当数字输出会让整条结果序列化失败
				if isNumericNonFinite(s) {
					return s
				}
				return json.Number(s)
			}
			return Normalize(dv)
		}
	}
	// pgtype.Date 等其余实现了 driver.Valuer 的类型转成标准值再归一化
	if valuer, ok := v.(driver.Valuer); ok {
		if dv, err := valuer.Value(); err == nil {
			switch dv.(type) {
			case nil, int64, float64, bool, string, []byte, time.Time:
				return Normalize(dv)
			}
		}
	}
	// 结构体枚举等其余类型退化为文本,保证可序列化
	return fmt.Sprintf("%v", v)
}

// formatUUID 把 16 字节 UUID 输出成标准文本形式, 与 PostgreSQL 的展示口径一致
func formatUUID(b [16]byte) string {

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// nonFiniteFloat 把非有限浮点数转成 PostgreSQL 的文本口径
func nonFiniteFloat(f float64) (string, bool) {

	switch {
	case math.IsNaN(f):
		return "NaN", true
	case math.IsInf(f, 1):
		return "Infinity", true
	case math.IsInf(f, -1):
		return "-Infinity", true
	}
	return "", false
}

// isNumericNonFinite 判断 numeric 的文本是否是非有限值
func isNumericNonFinite(s string) bool {

	switch strings.TrimSpace(s) {
	case "NaN", "Infinity", "-Infinity":
		return true
	}
	return false
}
