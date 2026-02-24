package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Params holds query parameters from an HTTP request.
type Params map[string]string

// RenderSQL evaluates a Tinybird-style Jinja-lite templated SQL string.
// Supports: {% if/elif/else/end %}, {{ String(...) }}, {{ Int32(...) }}, etc.
func RenderSQL(tmpl string, params Params) (string, error) {
	tokens := tokenize(tmpl)
	pos := 0
	nodes, err := parseNodeList(tokens, &pos)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	for _, n := range nodes {
		if err := n.render(params, &sb); err != nil {
			return "", err
		}
	}
	return sb.String(), nil
}

// ─── Tokenizer ────────────────────────────────────────────────────────────────

type tokKind int

const (
	tokText  tokKind = iota
	tokBlock         // {% ... %}
	tokExpr          // {{ ... }}
)

type tok struct {
	kind    tokKind
	content string // stripped inner content
}

func tokenize(s string) []tok {
	var tokens []tok
	for len(s) > 0 {
		bi := strings.Index(s, "{%")
		ei := strings.Index(s, "{{")

		if bi < 0 && ei < 0 {
			tokens = append(tokens, tok{tokText, s})
			break
		}

		// Pick the earlier delimiter
		next := bi
		isBlock := true
		if bi < 0 || (ei >= 0 && ei < bi) {
			next = ei
			isBlock = false
		}

		if next > 0 {
			tokens = append(tokens, tok{tokText, s[:next]})
		}
		s = s[next:]

		if isBlock {
			end := strings.Index(s, "%}")
			if end < 0 {
				tokens = append(tokens, tok{tokText, s})
				break
			}
			content := strings.TrimSpace(s[2:end])
			tokens = append(tokens, tok{tokBlock, content})
			s = s[end+2:]
		} else {
			end := strings.Index(s, "}}")
			if end < 0 {
				tokens = append(tokens, tok{tokText, s})
				break
			}
			content := strings.TrimSpace(s[2:end])
			tokens = append(tokens, tok{tokExpr, content})
			s = s[end+2:]
		}
	}
	return tokens
}

// ─── AST ──────────────────────────────────────────────────────────────────────

type astNode interface {
	render(params Params, sb *strings.Builder) error
}

type textNode struct{ text string }
type exprNode struct{ expr string }
type ifNode struct {
	cond     string
	thenBody []astNode
	elifs    []elifClause
	elseBody []astNode
}
type elifClause struct {
	cond string
	body []astNode
}

func (n *textNode) render(_ Params, sb *strings.Builder) error {
	sb.WriteString(n.text)
	return nil
}

func (n *exprNode) render(params Params, sb *strings.Builder) error {
	val, err := evalExpr(n.expr, params)
	if err != nil {
		return err
	}
	sb.WriteString(val)
	return nil
}

func (n *ifNode) render(params Params, sb *strings.Builder) error {
	if evalCond(n.cond, params) {
		for _, child := range n.thenBody {
			if err := child.render(params, sb); err != nil {
				return err
			}
		}
		return nil
	}
	for _, elif := range n.elifs {
		if evalCond(elif.cond, params) {
			for _, child := range elif.body {
				if err := child.render(params, sb); err != nil {
					return err
				}
			}
			return nil
		}
	}
	for _, child := range n.elseBody {
		if err := child.render(params, sb); err != nil {
			return err
		}
	}
	return nil
}

// ─── Parser ───────────────────────────────────────────────────────────────────

// parseNodeList parses tokens into a list of AST nodes, stopping when it hits
// {% end %}, {% else %}, or {% elif ... %} (which belong to the parent if-node).
func parseNodeList(tokens []tok, pos *int) ([]astNode, error) {
	var nodes []astNode
	for *pos < len(tokens) {
		t := tokens[*pos]
		switch t.kind {
		case tokText:
			nodes = append(nodes, &textNode{t.content})
			*pos++
		case tokExpr:
			nodes = append(nodes, &exprNode{t.content})
			*pos++
		case tokBlock:
			c := t.content
			// Stop signals for the parent parseIf call
			if c == "end" || c == "else" || strings.HasPrefix(c, "elif ") {
				return nodes, nil
			}
			if strings.HasPrefix(c, "if ") {
				*pos++
				ifN, err := parseIf(strings.TrimSpace(c[3:]), tokens, pos)
				if err != nil {
					return nil, err
				}
				nodes = append(nodes, ifN)
			} else {
				// Unknown/unsupported block tag — skip
				*pos++
			}
		}
	}
	return nodes, nil
}

func parseIf(cond string, tokens []tok, pos *int) (*ifNode, error) {
	thenBody, err := parseNodeList(tokens, pos)
	if err != nil {
		return nil, err
	}
	n := &ifNode{cond: cond, thenBody: thenBody}

	for *pos < len(tokens) {
		t := tokens[*pos]
		if t.kind != tokBlock {
			break
		}
		switch {
		case t.content == "end":
			*pos++
			return n, nil

		case t.content == "else":
			*pos++
			elseBody, err := parseNodeList(tokens, pos)
			if err != nil {
				return nil, err
			}
			n.elseBody = elseBody
			// consume the {% end %}
			if *pos < len(tokens) && tokens[*pos].kind == tokBlock && tokens[*pos].content == "end" {
				*pos++
			}
			return n, nil

		case strings.HasPrefix(t.content, "elif "):
			elifCond := strings.TrimSpace(t.content[5:])
			*pos++
			elifBody, err := parseNodeList(tokens, pos)
			if err != nil {
				return nil, err
			}
			n.elifs = append(n.elifs, elifClause{elifCond, elifBody})

		default:
			// Shouldn't happen, but bail out
			return n, nil
		}
	}
	return n, nil
}

// ─── Condition Evaluator ──────────────────────────────────────────────────────
//
// Grammar (informal):
//   expr     = or_expr
//   or_expr  = and_expr (' or ' and_expr)*
//   and_expr = not_expr (' and ' not_expr)*
//   not_expr = 'not ' not_expr | primary
//   primary  = defined(x) | x == 'v' | x != 'v' | x   (truthy check)

func evalCond(cond string, params Params) bool {
	cond = strings.TrimSpace(cond)

	// Top-level OR (not inside parens)
	if idx := topLevelIndex(cond, " or "); idx >= 0 {
		return evalCond(cond[:idx], params) || evalCond(cond[idx+4:], params)
	}

	// Top-level AND
	if idx := topLevelIndex(cond, " and "); idx >= 0 {
		return evalCond(cond[:idx], params) && evalCond(cond[idx+5:], params)
	}

	// NOT
	if strings.HasPrefix(cond, "not ") {
		return !evalCond(cond[4:], params)
	}

	// defined(x)
	if strings.HasPrefix(cond, "defined(") && strings.HasSuffix(cond, ")") {
		name := cond[8 : len(cond)-1]
		_, ok := params[name]
		return ok
	}

	// x == 'value'
	if idx := strings.Index(cond, " == "); idx >= 0 {
		left := strings.TrimSpace(cond[:idx])
		right := stripQuotes(strings.TrimSpace(cond[idx+4:]))
		return params[left] == right
	}

	// x != 'value'
	if idx := strings.Index(cond, " != "); idx >= 0 {
		left := strings.TrimSpace(cond[:idx])
		right := stripQuotes(strings.TrimSpace(cond[idx+4:]))
		return params[left] != right
	}

	// Bare identifier: truthy if present and not falsy
	val, ok := params[cond]
	if !ok {
		return false
	}
	return val != "" && val != "false" && val != "0"
}

// topLevelIndex finds the leftmost occurrence of sep that is not inside parentheses.
func topLevelIndex(s, sep string) int {
	depth := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && s[i:i+len(sep)] == sep {
			return i
		}
	}
	return -1
}

func stripQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// ─── Expression Evaluator ─────────────────────────────────────────────────────
// Handles: String(x), String(x,"default"), Int32(x,d), Float64(x,d),
//          DateTime(x,"d"), UInt8(x,d), Boolean(x), etc.

func evalExpr(expr string, params Params) (string, error) {
	paren := strings.Index(expr, "(")
	if paren < 0 {
		return "", fmt.Errorf("invalid template expression: %q", expr)
	}

	fn := strings.TrimSpace(expr[:paren])
	argStr := expr[paren+1:]
	if strings.HasSuffix(argStr, ")") {
		argStr = argStr[:len(argStr)-1]
	}

	parts := splitArgs(argStr)
	if len(parts) == 0 {
		return "", fmt.Errorf("no args in expression: %q", expr)
	}

	paramName := strings.TrimSpace(parts[0])
	var defaultVal string
	if len(parts) > 1 {
		defaultVal = stripQuotes(strings.TrimSpace(parts[1]))
	}

	val, ok := params[paramName]
	if !ok {
		val = defaultVal
	}

	switch fn {
	case "String":
		return escapeString(val), nil

	case "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64":
		if val == "" {
			if defaultVal == "" {
				return "0", nil
			}
			return defaultVal, nil
		}
		if _, err := strconv.ParseInt(val, 10, 64); err == nil {
			return val, nil
		}
		// Might be a float string — truncate
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return strconv.FormatInt(int64(f), 10), nil
		}
		return "0", nil

	case "Float32", "Float64":
		if val == "" {
			if defaultVal == "" {
				return "0", nil
			}
			return defaultVal, nil
		}
		if _, err := strconv.ParseFloat(val, 64); err != nil {
			return "0", nil
		}
		return val, nil

	case "DateTime", "DateTime64":
		if val == "" {
			if defaultVal == "" {
				return "'1970-01-01 00:00:00'", nil
			}
			return "'" + defaultVal + "'", nil
		}
		return "'" + normalizeDateTime(val) + "'", nil

	case "Boolean", "Bool":
		if val == "true" || val == "1" {
			return "1", nil
		}
		if val == "" && (defaultVal == "true" || defaultVal == "1") {
			return "1", nil
		}
		return "0", nil

	default:
		// Fallback: treat as string
		return escapeString(val), nil
	}
}

func escapeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

var datetimeFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

func normalizeDateTime(s string) string {
	for _, f := range datetimeFormats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC().Format("2006-01-02 15:04:05")
		}
	}
	return s // already in ClickHouse format hopefully
}

// splitArgs splits "arg1, arg2" respecting quotes and nested parens.
func splitArgs(s string) []string {
	var parts []string
	var cur strings.Builder
	depth := 0
	inQuote := byte(0)

	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote != 0 {
			cur.WriteByte(c)
			if c == inQuote && (i == 0 || s[i-1] != '\\') {
				inQuote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			inQuote = c
			cur.WriteByte(c)
		case '(':
			depth++
			cur.WriteByte(c)
		case ')':
			depth--
			cur.WriteByte(c)
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(cur.String()))
				cur.Reset()
				continue
			}
			cur.WriteByte(c)
		default:
			cur.WriteByte(c)
		}
	}
	if t := strings.TrimSpace(cur.String()); t != "" {
		parts = append(parts, t)
	}
	return parts
}
