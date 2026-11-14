// 连接属性解析:配置里的连接属性沿用 JDBC 写法,这里统一解析后由各驱动映射到原生参数
package driver

import (
	"net/url"
	"strings"
)

// ParseProperties 解析 key=value&key=value 形式的连接属性
// 值可能被 URL 编码,这里解码后返回小写键的映射
func ParseProperties(raw string) map[string]string {

	props := make(map[string]string)
	for _, part := range strings.Split(raw, "&") {
		segment := strings.TrimSpace(part)
		if segment == "" {
			continue
		}
		kv := strings.SplitN(segment, "=", 2)
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		if key == "" {
			continue
		}
		value := ""
		if len(kv) == 2 {
			value = strings.TrimSpace(kv[1])
			if decoded, err := url.QueryUnescape(value); err == nil {
				value = decoded
			}
		}
		props[key] = value
	}
	return props
}

// HasAny 判断连接属性里是否出现过任意一个键
// 用于区分“没配”与“配了 0”: 连接属性与连接池参数冲突时以连接属性为准
func HasAny(props map[string]string, keys ...string) bool {

	for _, key := range keys {
		if _, ok := props[key]; ok {
			return true
		}
	}
	return false
}

// SplitSessionVariables 解析 sessionVariables 里的多个变量
// 形如 time_zone='+08:00';sql_mode='ANSI'
func SplitSessionVariables(raw string) map[string]string {

	vars := make(map[string]string)
	for _, part := range strings.Split(raw, ";") {
		segment := strings.TrimSpace(part)
		if segment == "" {
			continue
		}
		kv := strings.SplitN(segment, "=", 2)
		key := strings.TrimSpace(kv[0])
		if key == "" {
			continue
		}
		value := ""
		if len(kv) == 2 {
			value = strings.Trim(strings.TrimSpace(kv[1]), "'\"")
		}
		vars[key] = value
	}
	return vars
}
