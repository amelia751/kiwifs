package workflow

import (
	"bytes"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type TaskStatus string

const (
	TaskTodo       TaskStatus = "todo"
	TaskInProgress TaskStatus = "in-progress"
	TaskDone       TaskStatus = "done"
	TaskBlocked    TaskStatus = "blocked"
)

type Task struct {
	ID          string     `json:"id" yaml:"id"`
	Title       string     `json:"title" yaml:"title"`
	Status      TaskStatus `json:"status" yaml:"status"`
	Assignee    string     `json:"assignee,omitempty" yaml:"assignee,omitempty"`
	DueDate     string     `json:"dueDate,omitempty" yaml:"due-date,omitempty"`
	CompletedAt string     `json:"completedAt,omitempty" yaml:"completed-at,omitempty"`
	CompletedBy string     `json:"completedBy,omitempty" yaml:"completed-by,omitempty"`
}

type ApprovalStatus string

const (
	ApprovalPending  ApprovalStatus = "pending"
	ApprovalApproved ApprovalStatus = "approved"
	ApprovalRejected ApprovalStatus = "rejected"
)

type Approval struct {
	Status   ApprovalStatus `json:"status" yaml:"status"`
	Approver string         `json:"approver,omitempty" yaml:"approver,omitempty"`
	Date     string         `json:"date,omitempty" yaml:"date,omitempty"`
	Comment  string         `json:"comment,omitempty" yaml:"comment,omitempty"`
}

type WorkflowMeta struct {
	Type     string    `json:"type" yaml:"type"`
	Tasks    []Task    `json:"tasks" yaml:"tasks"`
	Approval *Approval `json:"approval,omitempty" yaml:"approval,omitempty"`
	DueDate  string    `json:"dueDate,omitempty" yaml:"due-date,omitempty"`
	Progress float64   `json:"progress"`
}

// ParseWorkflow extracts workflow metadata from markdown frontmatter.
// Returns nil when no tasks field is present.
func ParseWorkflow(content []byte) (*WorkflowMeta, error) {
	fm, _, err := splitFrontmatter(content)
	if err != nil {
		return nil, err
	}
	if len(fm) == 0 {
		return nil, nil
	}

	var meta WorkflowMeta
	if err := yaml.Unmarshal(fm, &meta); err != nil {
		return nil, fmt.Errorf("parse workflow frontmatter: %w", err)
	}
	if len(meta.Tasks) == 0 {
		return nil, nil
	}

	done := 0
	for _, t := range meta.Tasks {
		if t.Status == TaskDone {
			done++
		}
	}
	meta.Progress = float64(done) / float64(len(meta.Tasks))
	return &meta, nil
}

// UpdateTask updates a task's status in the frontmatter and returns the
// rewritten content. Sets completedAt/completedBy when transitioning to done.
func UpdateTask(content []byte, taskID string, status TaskStatus, actor string) ([]byte, error) {
	fm, body, err := splitFrontmatter(content)
	if err != nil {
		return nil, err
	}
	if len(fm) == 0 {
		return nil, fmt.Errorf("no frontmatter found")
	}

	var root yaml.Node
	if err := yaml.Unmarshal(fm, &root); err != nil {
		return nil, fmt.Errorf("parse frontmatter: %w", err)
	}
	mapping := ensureMappingDocument(&root)

	tasksNode := findMappingValue(mapping, "tasks")
	if tasksNode == nil || tasksNode.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("no tasks sequence in frontmatter")
	}

	found := false
	for _, taskNode := range tasksNode.Content {
		if taskNode.Kind != yaml.MappingNode {
			continue
		}
		idVal := findMappingValue(taskNode, "id")
		if idVal == nil || idVal.Value != taskID {
			continue
		}
		found = true
		setMappingValue(taskNode, "status", string(status))
		if status == TaskDone {
			setMappingValue(taskNode, "completed-at", time.Now().UTC().Format(time.RFC3339))
			if actor != "" {
				setMappingValue(taskNode, "completed-by", actor)
			}
		}
		break
	}
	if !found {
		return nil, fmt.Errorf("task %q not found", taskID)
	}

	return serializeFrontmatter(&root, body)
}

// MergeFrontmatter applies a set of key → value updates to the YAML
// frontmatter of content, preserving other keys and the body. A nil value
// (or explicit null) removes the key. If content has no frontmatter block,
// one is created.
func MergeFrontmatter(content []byte, updates map[string]any) ([]byte, error) {
	fm, body, err := splitFrontmatter(content)
	if err != nil {
		return nil, err
	}

	var root yaml.Node
	if len(fm) > 0 {
		if err := yaml.Unmarshal(fm, &root); err != nil {
			return nil, fmt.Errorf("parse frontmatter: %w", err)
		}
	}
	// splitFrontmatter returns body == content when there is no frontmatter
	// block; in that case we still want to emit frontmatter at the top.
	if len(fm) == 0 {
		body = content
	}
	mapping := ensureMappingDocument(&root)

	for key, val := range updates {
		if val == nil {
			removeMappingKey(mapping, key)
			continue
		}
		if err := setMappingValueAny(mapping, key, val); err != nil {
			return nil, fmt.Errorf("set %q: %w", key, err)
		}
	}

	return serializeFrontmatter(&root, body)
}

func removeMappingKey(mapping *yaml.Node, key string) {
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			mapping.Content = append(mapping.Content[:i], mapping.Content[i+2:]...)
			return
		}
	}
}

func setMappingValueAny(mapping *yaml.Node, key string, value any) error {
	node, err := buildYAMLNode(value)
	if err != nil {
		return err
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			*mapping.Content[i+1] = *node
			return nil
		}
	}
	k := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
	mapping.Content = append(mapping.Content, k, node)
	return nil
}

func buildYAMLNode(value any) (*yaml.Node, error) {
	n := &yaml.Node{}
	if err := n.Encode(value); err != nil {
		return nil, err
	}
	return n, nil
}

// UpdateApproval sets the approval status in frontmatter and returns new content.
func UpdateApproval(content []byte, status ApprovalStatus, approver, comment string) ([]byte, error) {
	fm, body, err := splitFrontmatter(content)
	if err != nil {
		return nil, err
	}
	if len(fm) == 0 {
		return nil, fmt.Errorf("no frontmatter found")
	}

	var root yaml.Node
	if err := yaml.Unmarshal(fm, &root); err != nil {
		return nil, fmt.Errorf("parse frontmatter: %w", err)
	}
	mapping := ensureMappingDocument(&root)

	approvalNode := findMappingValue(mapping, "approval")
	if approvalNode == nil {
		approvalNode = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "approval"}
		mapping.Content = append(mapping.Content, key, approvalNode)
	}
	if approvalNode.Kind != yaml.MappingNode {
		n := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		*approvalNode = *n
	}

	setMappingValue(approvalNode, "status", string(status))
	if approver != "" {
		setMappingValue(approvalNode, "approver", approver)
	}
	setMappingValue(approvalNode, "date", time.Now().UTC().Format(time.RFC3339))
	if comment != "" {
		setMappingValue(approvalNode, "comment", comment)
	}

	return serializeFrontmatter(&root, body)
}

// --- YAML helpers ---

func ensureMappingDocument(root *yaml.Node) *yaml.Node {
	if root.Kind == 0 {
		root.Kind = yaml.DocumentNode
	}
	if root.Kind != yaml.DocumentNode {
		wrapped := *root
		root.Kind = yaml.DocumentNode
		root.Content = []*yaml.Node{&wrapped}
	}
	if len(root.Content) == 0 {
		mapping := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		root.Content = append(root.Content, mapping)
		return mapping
	}
	return root.Content[0]
}

func findMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			return mapping.Content[i+1]
		}
	}
	return nil
}

func setMappingValue(mapping *yaml.Node, key, value string) {
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			mapping.Content[i+1].Value = value
			mapping.Content[i+1].Kind = yaml.ScalarNode
			mapping.Content[i+1].Tag = "!!str"
			return
		}
	}
	k := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
	v := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
	mapping.Content = append(mapping.Content, k, v)
}

func serializeFrontmatter(root *yaml.Node, body []byte) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		return nil, fmt.Errorf("serialise frontmatter: %w", err)
	}
	_ = enc.Close()

	var out bytes.Buffer
	out.WriteString("---\n")
	out.Write(bytes.TrimRight(buf.Bytes(), "\n"))
	out.WriteString("\n---\n")
	out.Write(body)
	return out.Bytes(), nil
}

// splitFrontmatter returns the YAML block (without delimiters) and the body.
func splitFrontmatter(content []byte) (fm, body []byte, err error) {
	delim := []byte("---")

	line, rest, ok := bytes.Cut(content, []byte("\n"))
	if !ok {
		return nil, content, nil
	}
	if !bytes.Equal(bytes.TrimRight(line, "\r"), delim) {
		return nil, content, nil
	}
	scanner := rest
	pos := 0
	for {
		nl := bytes.IndexByte(scanner, '\n')
		var current []byte
		if nl < 0 {
			current = scanner
		} else {
			current = scanner[:nl]
		}
		if bytes.Equal(bytes.TrimRight(current, "\r"), delim) {
			fm = rest[:pos]
			if nl < 0 {
				body = nil
			} else {
				body = scanner[nl+1:]
			}
			return fm, body, nil
		}
		if nl < 0 {
			return nil, nil, nil
		}
		pos += nl + 1
		scanner = scanner[nl+1:]
	}
}
