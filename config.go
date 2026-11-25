package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// CloudConfig holds the Keystone v3 password credentials + client options.
type CloudConfig struct {
	AuthURL           string
	Username          string
	UserID            string
	Password          string
	ProjectName       string
	ProjectID         string
	UserDomainName    string
	UserDomainID      string
	ProjectDomainName string
	ProjectDomainID   string
	Region            string
	Interface         string
	CACert            string
	Insecure          bool
}

// loadCloud reads clouds.yaml (if present) and lets OS_* env vars override.
func loadCloud(path, cloud string) (CloudConfig, error) {
	var c CloudConfig
	if data, err := os.ReadFile(path); err == nil {
		root, err := parseSimpleYAML(string(data))
		if err != nil {
			return c, fmt.Errorf("parse %s: %w", path, err)
		}
		clouds, _ := root["clouds"].(yNode)
		if cloud == "" && len(clouds) == 1 {
			for name := range clouds {
				cloud = name
			}
		}
		if cloud != "" {
			node, ok := clouds[cloud].(yNode)
			if !ok {
				names := make([]string, 0, len(clouds))
				for n := range clouds {
					names = append(names, n)
				}
				sort.Strings(names)
				return c, fmt.Errorf("cloud %q not found in %s (available: %s)", cloud, path, strings.Join(names, ", "))
			}
			auth, _ := node["auth"].(yNode)
			s := func(n yNode, k string) string { v, _ := n[k].(string); return v }
			c.AuthURL = s(auth, "auth_url")
			c.Username = s(auth, "username")
			c.UserID = s(auth, "user_id")
			c.Password = s(auth, "password")
			c.ProjectName = firstNonEmpty(s(auth, "project_name"), s(auth, "tenant_name"))
			c.ProjectID = firstNonEmpty(s(auth, "project_id"), s(auth, "tenant_id"))
			c.UserDomainName = firstNonEmpty(s(auth, "user_domain_name"), s(auth, "domain_name"))
			c.UserDomainID = firstNonEmpty(s(auth, "user_domain_id"), s(auth, "domain_id"))
			c.ProjectDomainName = firstNonEmpty(s(auth, "project_domain_name"), s(auth, "domain_name"))
			c.ProjectDomainID = firstNonEmpty(s(auth, "project_domain_id"), s(auth, "domain_id"))
			c.Region = firstNonEmpty(s(node, "region_name"), s(node, "region"))
			c.Interface = firstNonEmpty(s(node, "interface"), s(node, "endpoint_type"))
			c.CACert = s(node, "cacert")
			if v := strings.ToLower(s(node, "verify")); v == "false" || v == "no" {
				c.Insecure = true
			}
		}
	}
	over := func(dst *string, env string) {
		if v := os.Getenv(env); v != "" {
			*dst = v
		}
	}
	over(&c.AuthURL, "OS_AUTH_URL")
	over(&c.Username, "OS_USERNAME")
	over(&c.UserID, "OS_USER_ID")
	over(&c.Password, "OS_PASSWORD")
	over(&c.ProjectName, "OS_PROJECT_NAME")
	over(&c.ProjectID, "OS_PROJECT_ID")
	over(&c.UserDomainName, "OS_USER_DOMAIN_NAME")
	over(&c.UserDomainID, "OS_USER_DOMAIN_ID")
	over(&c.ProjectDomainName, "OS_PROJECT_DOMAIN_NAME")
	over(&c.ProjectDomainID, "OS_PROJECT_DOMAIN_ID")
	over(&c.Region, "OS_REGION_NAME")
	over(&c.Interface, "OS_INTERFACE")
	over(&c.CACert, "OS_CACERT")

	if c.AuthURL == "" || c.Password == "" || (c.Username == "" && c.UserID == "") {
		return c, fmt.Errorf("incomplete credentials: need auth_url, username/user_id, password (clouds file %q, cloud %q, or OS_* env)", path, cloud)
	}
	if c.ProjectName == "" && c.ProjectID == "" {
		return c, fmt.Errorf("project_name/project_id required (token must be project-scoped to get the service catalog)")
	}
	return c, nil
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// yNode is a minimal nested map used for clouds.yaml (maps + scalars only).
type yNode map[string]any

// parseSimpleYAML parses the small YAML subset used by clouds.yaml:
// nested mappings with scalar values, comments, and quoted strings.
// Lists and multi-line scalars are ignored.
func parseSimpleYAML(data string) (yNode, error) {
	type frame struct {
		indent int
		node   yNode
	}
	root := yNode{}
	stack := []frame{{-1, root}}
	for i, raw := range strings.Split(data, "\n") {
		line := strings.TrimRight(stripComment(raw), " \t\r")
		trim := strings.TrimSpace(line)
		if trim == "" || trim == "---" || trim == "..." {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if strings.Contains(line[:indent], "\t") {
			return nil, fmt.Errorf("line %d: tabs are not allowed for indentation", i+1)
		}
		if strings.HasPrefix(trim, "- ") || trim == "-" {
			continue
		}
		k, v, ok := splitKV(trim)
		if !ok {
			return nil, fmt.Errorf("line %d: expected 'key: value'", i+1)
		}
		for len(stack) > 1 && indent <= stack[len(stack)-1].indent {
			stack = stack[:len(stack)-1]
		}
		parent := stack[len(stack)-1].node
		if v == "" {
			child := yNode{}
			parent[k] = child
			stack = append(stack, frame{indent, child})
		} else {
			parent[k] = unquote(v)
		}
	}
	return root, nil
}

func stripComment(s string) string {
	var q rune
	for i, r := range s {
		switch {
		case q != 0:
			if r == q {
				q = 0
			}
		case r == '"' || r == '\'':
			q = r
		case r == '#' && (i == 0 || s[i-1] == ' ' || s[i-1] == '\t'):
			return s[:i]
		}
	}
	return s
}

func splitKV(s string) (string, string, bool) {
	var q rune
	for i, r := range s {
		switch {
		case q != 0:
			if r == q {
				q = 0
			}
		case r == '"' || r == '\'':
			q = r
		case r == ':' && (i+1 == len(s) || s[i+1] == ' ' || s[i+1] == '\t'):
			return unquote(strings.TrimSpace(s[:i])), strings.TrimSpace(s[i+1:]), true
		}
	}
	return "", "", false
}

func unquote(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
		return v[1 : len(v)-1]
	}
	return v
}
