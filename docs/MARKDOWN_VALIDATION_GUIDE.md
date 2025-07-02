# Markdown Validation Guide for Teranode Documentation

This guide provides comprehensive solutions to prevent and catch markdown formatting issues that cause problems in Material MkDocs rendering.

## 🛠️ Available Validation Tools

### 1. Pre-commit Hooks (Automatic)

**Setup**: Already configured in `.pre-commit-config.yaml`
- **markdownlint**: Catches formatting issues before commits
- **Configuration**: `.markdownlint.yml` with rules tailored for Material MkDocs

**Usage**:
```bash
# Install pre-commit hooks
pre-commit install

# Run manually on all files
pre-commit run markdownlint --all-files
```

### 2. Custom Validation Script

**Location**: `scripts/validate-markdown.py`

**Features**:

- Detects missing blank lines before lists
- Finds inconsistent nested bullet indentation
- Identifies configuration parameters missing Type/Default/Impact details
- Can automatically fix issues with `--fix` flag

**Usage**:
```bash
# Validate all documentation
python3 scripts/validate-markdown.py docs/

# Validate and auto-fix issues
python3 scripts/validate-markdown.py docs/ --fix

# Validate single file
python3 scripts/validate-markdown.py docs/topics/services/alert.md
```

### 3. VS Code Extensions (Real-time)

**Recommended Extensions**:

- **markdownlint**: Real-time linting in editor
- **Markdown All in One**: Preview and formatting
- **Material Theme**: Better syntax highlighting

**Setup**:
1. Install extensions
2. Add to VS Code settings.json:
```json
{
  "markdownlint.config": {
    "MD007": { "indent": 4 },
    "MD032": true,
    "MD013": false
  }
}
```

## 🎯 Common Issues & Prevention

### Issue 1: Missing Blank Lines Before Lists

**Problem**: Lists render inline instead of as proper lists
**Solution**: Always add blank line before lists

```markdown
❌ Wrong:
Configuration options:

- Option 1
- Option 2

✅ Correct:
Configuration options:

- Option 1
- Option 2
```

### Issue 2: Inconsistent Nested Indentation

**Problem**: Nested items don't render as nested
**Solution**: Use exactly 4 spaces for nested items

```markdown
❌ Wrong:

- Main item
  - Nested item (2 spaces)

✅ Correct:

- Main item
    - Nested item (4 spaces)
```

### Issue 3: Configuration Parameter Formatting

**Problem**: Inconsistent parameter documentation
**Solution**: Use standard format for all parameters

```markdown
✅ Standard Format:
1. **`parameter_name`**: Brief description.
    - Type: string
    - Default: "value"
    - Impact: Detailed explanation of what this controls.
```

## 🚀 Workflow Integration

### Daily Development
1. **VS Code Extensions**: Real-time feedback while editing
2. **Pre-commit Hooks**: Automatic validation before commits
3. **Custom Script**: Bulk validation and fixes

### CI/CD Pipeline
1. **GitHub Actions**: Validate all PRs
2. **Deployment**: Only deploy if validation passes

### Periodic Maintenance
```bash
# Weekly validation run
python3 scripts/validate-markdown.py docs/ --fix
git add -A
git commit -m "docs: fix markdown formatting issues"
```

## 📋 Validation Checklist

Before committing documentation changes:

- [ ] Run custom validation script
- [ ] Check pre-commit hooks pass
- [ ] Preview in Material MkDocs locally
- [ ] Verify nested lists render correctly
- [ ] Confirm configuration parameters follow standard format

## 🔧 Quick Fixes

**Fix all formatting issues in docs**:
```bash
python3 scripts/validate-markdown.py docs/ --fix
```

**Run markdownlint**:
```bash
markdownlint docs/ --config .markdownlint.yml --fix
```

**Preview locally**:
```bash
mkdocs serve
# Open http://localhost:8000
```

This multi-layered approach ensures high-quality, consistently formatted documentation that renders perfectly in Material MkDocs.
