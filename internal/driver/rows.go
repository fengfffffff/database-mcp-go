// 行组装:按列顺序组装一行,重名列自动加序号后缀
package driver

import (
	"fmt"

	"github.com/fengfffffff/database-mcp-go/internal/model"
)

// BuildRow 组装一行数据
// 重名列不能直接覆盖,join 场景下 a.id 与 b.id 都要保留,从第二列起依次改名为 id_2、id_3
func BuildRow(columns []string, values []any) model.OrderedRow {

	cols := make([]string, 0, len(columns))
	vals := make([]any, 0, len(values))
	index := make(map[string]int, len(columns))
	for i, name := range columns {
		var v any
		if i < len(values) {
			v = Normalize(values[i])
		}
		unique := name
		if _, exists := index[unique]; exists {
			for seq := 2; ; seq++ {
				candidate := fmt.Sprintf("%s_%d", name, seq)
				if _, taken := index[candidate]; !taken {
					unique = candidate
					break
				}
			}
		}
		index[unique] = len(cols)
		cols = append(cols, unique)
		vals = append(vals, v)
	}
	return model.OrderedRow{Columns: cols, Values: vals}
}
