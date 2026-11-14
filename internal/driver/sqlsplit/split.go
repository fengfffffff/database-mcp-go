// SQL 语句切分:按分号拆分多条语句,正确跳过引号、方括号、注释与美元引用内的分号
// 各库拆句都是为了精确定位失败语句与超时语句, 并让写操作逐条拿到受影响行数
package sqlsplit

import (
	"strings"
	"unicode"
)

// Split 按 MySQL 语法切分
func Split(sqlText string) []string {

	return split(sqlText, dialect{})
}

// SplitPostgres 按 PostgreSQL 语法切分,额外识别 $$ 与 $tag$ 包裹的字符串
func SplitPostgres(sqlText string) []string {

	return split(sqlText, dialect{postgres: true})
}

// SplitTSQL 按 SQL Server 语法切分,识别方括号标识符,不把井号当注释,反斜杠不作转义
func SplitTSQL(sqlText string) []string {

	return split(sqlText, dialect{tsql: true})
}

// SplitSQLite 按 SQLite 语法切分,识别方括号与反引号标识符,不把井号当注释,反斜杠不作转义
func SplitSQLite(sqlText string) []string {

	return split(sqlText, dialect{sqlite: true})
}

// dialect 各方言的语法差异开关
type dialect struct {
	// postgres 启用美元引用与嵌套块注释
	postgres bool
	// tsql 启用方括号标识符与嵌套块注释, 关闭井号注释与反斜杠转义
	tsql bool
	// sqlite 启用方括号标识符, 关闭井号注释与反斜杠转义
	sqlite bool
}

// nestedComments 块注释是否允许嵌套
// PostgreSQL 与 SQL Server 都支持嵌套, MySQL 不支持, 后者里内层 /* 只是注释文本
func (d dialect) nestedComments() bool {

	return d.postgres || d.tsql
}

// split 通用切分实现
func split(sqlText string, d dialect) []string {

	stmts := make([]string, 0, 4)
	var sb strings.Builder
	var dollarTag []rune
	blockDepth := 0
	const (
		normal     = iota
		inSingle   // '...'
		inDouble   // "..."
		inBacktick // `...`
		inBracket  // [...] SQL Server 的标识符引用
		inLine     // -- 或 # 行注释
		inBlock    // /* ... */
		inDollar   // $$...$$ 或 $tag$...$tag$
	)
	state := normal
	runes := []rune(sqlText)

	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		switch state {
		case normal:
			switch ch {
			case '\'':
				state = inSingle
				sb.WriteRune(ch)
			case '"':
				state = inDouble
				sb.WriteRune(ch)
			case '`':
				// 反引号是 MySQL 的标识符引用, SQL Server 也认, 只有 PostgreSQL 里是普通字符
				if d.postgres {
					sb.WriteRune(ch)
				} else {
					state = inBacktick
					sb.WriteRune(ch)
				}
			case '[':
				// 方括号是 SQL Server 与 SQLite 的标识符引用,内部的分号不切
				if d.tsql || d.sqlite {
					state = inBracket
				}
				sb.WriteRune(ch)
			case '#':
				// 井号是 MySQL 的行注释, SQL Server 里 # 是临时表前缀, PostgreSQL 与 SQLite 里是普通字符
				if d.postgres || d.tsql || d.sqlite {
					sb.WriteRune(ch)
				} else {
					state = inLine
					sb.WriteRune(ch)
				}
			case '$':
				// 美元引用只在 PostgreSQL 启用,$$ 与 $tag$ 之间的分号不切
				if d.postgres {
					if tag, ok := dollarTagAt(runes, i); ok {
						state = inDollar
						// 按字符数记录标签长度, 标签里允许非 ASCII 字母, 用字节数会跳过头
						dollarTag = []rune(tag)
						sb.WriteString(tag)
						i += len(dollarTag) - 1
						break
					}
				}
				sb.WriteRune(ch)
			case '-':
				// MySQL 的行注释要求 -- 后跟空白或控制字符,其余方言 -- 后直接是注释内容
				if i+1 < len(runes) && runes[i+1] == '-' {
					if d.postgres || d.tsql || d.sqlite || i+2 >= len(runes) || runes[i+2] == ' ' || runes[i+2] == '\t' || runes[i+2] == '\n' || runes[i+2] == '\r' {
						state = inLine
						sb.WriteString("--")
						i++
						break
					}
				}
				sb.WriteRune(ch)
			case '/':
				if i+1 < len(runes) && runes[i+1] == '*' {
					state = inBlock
					blockDepth = 1
					sb.WriteString("/*")
					i++
					break
				}
				sb.WriteRune(ch)
			case ';':
				// 语句结束,分号本身不进入语句文本
				appendStmt(&stmts, sb.String())
				sb.Reset()
			default:
				sb.WriteRune(ch)
			}
		case inSingle:
			sb.WriteRune(ch)
			// 反斜杠转义是 MySQL 的语法, SQL Server 与 SQLite 里就是普通字符
			if ch == '\\' && !d.tsql && !d.sqlite && i+1 < len(runes) {
				i++
				sb.WriteRune(runes[i])
				break
			}
			if ch == '\'' {
				// 连续两个单引号是转义,仍处于字符串内
				if i+1 < len(runes) && runes[i+1] == '\'' {
					i++
					sb.WriteRune(runes[i])
					break
				}
				state = normal
			}
		case inDouble:
			sb.WriteRune(ch)
			if ch == '\\' && !d.tsql && !d.sqlite && i+1 < len(runes) {
				i++
				sb.WriteRune(runes[i])
				break
			}
			if ch == '"' {
				if i+1 < len(runes) && runes[i+1] == '"' {
					i++
					sb.WriteRune(runes[i])
					break
				}
				state = normal
			}
		case inBacktick:
			sb.WriteRune(ch)
			if ch == '`' {
				if i+1 < len(runes) && runes[i+1] == '`' {
					i++
					sb.WriteRune(runes[i])
					break
				}
				state = normal
			}
		case inBracket:
			sb.WriteRune(ch)
			if ch == ']' {
				// 连续两个右方括号是转义,仍处于标识符内
				if i+1 < len(runes) && runes[i+1] == ']' {
					i++
					sb.WriteRune(runes[i])
					break
				}
				state = normal
			}
		case inLine:
			sb.WriteRune(ch)
			if ch == '\n' {
				state = normal
			}
		case inBlock:
			sb.WriteRune(ch)
			// 嵌套方言里内层 /* 加深一层, 与外层 */ 配对后才算注释结束
			if ch == '/' && d.nestedComments() && i+1 < len(runes) && runes[i+1] == '*' {
				i++
				sb.WriteRune(runes[i])
				blockDepth++
				break
			}
			if ch == '*' && i+1 < len(runes) && runes[i+1] == '/' {
				i++
				sb.WriteRune(runes[i])
				blockDepth--
				if blockDepth <= 0 {
					state = normal
				}
			}
		case inDollar:
			sb.WriteRune(ch)
			if ch == '$' && hasRunePrefix(runes[i:], dollarTag) {
				// 补齐结束标记的剩余字符,整段引用原样保留
				for k := 1; k < len(dollarTag); k++ {
					sb.WriteRune(runes[i+k])
				}
				i += len(dollarTag) - 1
				state = normal
			}
		}
	}
	appendStmt(&stmts, sb.String())
	return stmts
}

// dollarTagAt 识别从 i 开始的美元引用起始标记,支持 $$ 与 $tag$
func dollarTagAt(runes []rune, i int) (string, bool) {

	// 前一个字符属于标识符时,这个 $ 是标识符的一部分,不是美元引用起始
	if i > 0 && (isTagPart(runes[i-1]) || runes[i-1] == '$') {
		return "", false
	}
	j := i + 1
	if j >= len(runes) {
		return "", false
	}
	if runes[j] == '$' {
		return "$$", true
	}
	if !isTagStart(runes[j]) {
		return "", false
	}
	for j < len(runes) && isTagPart(runes[j]) {
		j++
	}
	if j >= len(runes) || runes[j] != '$' {
		return "", false
	}
	return string(runes[i : j+1]), true
}

// isTagStart 标记首字符须为字母或下划线
func isTagStart(r rune) bool {

	return r == '_' || unicode.IsLetter(r)
}

// isTagPart 标记后续字符可为字母、数字或下划线
func isTagPart(r rune) bool {

	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// hasRunePrefix 判断 runes 是否以 prefix 开头
func hasRunePrefix(runes, prefix []rune) bool {

	if len(runes) < len(prefix) {
		return false
	}
	for i := range prefix {
		if runes[i] != prefix[i] {
			return false
		}
	}
	return true
}

// appendStmt 追加有效语句,纯注释或空白的片段不计入
func appendStmt(stmts *[]string, raw string) {

	stmt := strings.TrimSpace(raw)
	if stmt != "" && isEffective(stmt) {
		*stmts = append(*stmts, stmt)
	}
}

// isEffective 判断片段里是否含有可执行内容
// 纯注释片段不算语句,但 MySQL 的可执行注释 /*! ... */ 属于有效语句
func isEffective(stmt string) bool {

	s := strings.TrimSpace(stmt)
	for len(s) > 0 {
		switch {
		case strings.HasPrefix(s, "/*!"):
			return true
		case strings.HasPrefix(s, "--"):
			idx := strings.IndexByte(s, '\n')
			if idx < 0 {
				return false
			}
			s = strings.TrimSpace(s[idx+1:])
		case strings.HasPrefix(s, "#"):
			idx := strings.IndexByte(s, '\n')
			if idx < 0 {
				return false
			}
			s = strings.TrimSpace(s[idx+1:])
		case strings.HasPrefix(s, "/*"):
			idx := strings.Index(s, "*/")
			if idx < 0 {
				return false
			}
			s = strings.TrimSpace(s[idx+2:])
		default:
			return true
		}
	}
	return false
}
