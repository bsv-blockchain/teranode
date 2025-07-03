#!/usr/bin/env python3
"""
Markdown Validation Script for Teranode Documentation

This script validates markdown files for common formatting issues that cause
problems in Material MkDocs rendering, specifically:
- Missing blank lines before lists
- Inconsistent nested bullet point indentation
- Numbered list formatting issues
- Missing nested details for configuration parameters

Usage:
    python3 scripts/validate-markdown.py docs/
    python3 scripts/validate-markdown.py docs/ --fix
"""

import os
import re
import sys
import argparse
from pathlib import Path

class MarkdownValidator:
    def __init__(self, fix_mode=False):
        self.fix_mode = fix_mode
        self.issues_found = []

    def validate_file(self, filepath):
        """Validate a single markdown file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            lines = content.split('\n')
            file_issues = []
            modified = False

            for i, line in enumerate(lines):
                line_num = i + 1

                # Check 1: Missing blank line before lists
                if (line.strip().endswith(':') and
                    i + 1 < len(lines) and
                    (lines[i + 1].strip().startswith('- ') or lines[i + 1].strip().startswith('* '))):

                    issue = f"Line {line_num}: Missing blank line before list"
                    file_issues.append(issue)

                    if self.fix_mode:
                        lines.insert(i + 1, '')
                        modified = True

                # Check 2: Inconsistent nested bullet indentation
                if (line.startswith('  - ') and
                    not line.startswith('    - ') and
                    re.search(r'(Type|Impact|Example|Purpose|Note|Default|Usage|Description):', line)):

                    issue = f"Line {line_num}: Inconsistent nested bullet indentation (should be 4 spaces)"
                    file_issues.append(issue)

                    if self.fix_mode:
                        lines[i] = '    ' + line[2:]
                        modified = True

                # Check 2b: Numbered list sub-bullets with wrong indentation (3 spaces instead of 4)
                if (line.startswith('   - ') and
                    not line.startswith('    - ') and
                    i > 0 and re.match(r'^\d+\.\s+\*\*.*\*\*:', lines[i-2].strip() if i >= 2 else '')):

                    issue = f"Line {line_num}: Numbered list sub-bullet has wrong indentation (should be 4 spaces)"
                    file_issues.append(issue)

                    if self.fix_mode:
                        lines[i] = '    ' + line[3:]
                        modified = True

                # Check 2c: Extra blank lines between numbered items and their sub-bullets
                if (line.strip() == '' and
                    i > 0 and i + 1 < len(lines) and
                    re.match(r'^\d+\.\s+\*\*.*\*\*:', lines[i-1].strip()) and
                    lines[i+1].strip().startswith('   - ')):

                    issue = f"Line {line_num}: Extra blank line between numbered item and sub-bullets"
                    file_issues.append(issue)

                    if self.fix_mode:
                        lines.pop(i)
                        modified = True

                # Check 3: Configuration parameters missing nested details
                if (re.match(r'^\d+\.\s+\*\*.*\*\*:', line.strip()) and
                    i + 1 < len(lines) and
                    not lines[i + 1].strip().startswith('    - Type:') and
                    not lines[i + 1].strip() == ''):

                    issue = f"Line {line_num}: Configuration parameter may be missing Type/Default/Impact details"
                    file_issues.append(issue)

            # Write fixes if any were made
            if modified and self.fix_mode:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))
                print(f"✅ Fixed {len(file_issues)} issues in {filepath}")

            return file_issues

        except Exception as e:
            return [f"Error reading file: {e}"]

    def validate_directory(self, directory):
        """Validate all markdown files in a directory"""
        total_issues = 0
        files_with_issues = 0

        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.md'):
                    filepath = os.path.join(root, file)
                    issues = self.validate_file(filepath)

                    if issues:
                        files_with_issues += 1
                        total_issues += len(issues)
                        rel_path = os.path.relpath(filepath, directory)

                        if not self.fix_mode:
                            print(f"\n❌ {rel_path}:")
                            for issue in issues:
                                print(f"   {issue}")

        return total_issues, files_with_issues

    def run(self, target_path):
        """Run validation on target path"""
        if os.path.isfile(target_path):
            issues = self.validate_file(target_path)
            if issues and not self.fix_mode:
                print(f"\n❌ {target_path}:")
                for issue in issues:
                    print(f"   {issue}")
            return len(issues), 1 if issues else 0

        elif os.path.isdir(target_path):
            return self.validate_directory(target_path)

        else:
            print(f"Error: {target_path} is not a valid file or directory")
            return 0, 0

def main():
    parser = argparse.ArgumentParser(description='Validate Teranode markdown documentation')
    parser.add_argument('path', help='Path to markdown file or directory to validate')
    parser.add_argument('--fix', action='store_true', help='Automatically fix issues found')

    args = parser.parse_args()

    validator = MarkdownValidator(fix_mode=args.fix)
    total_issues, files_with_issues = validator.run(args.path)

    if args.fix:
        print(f"\n🔧 Fix mode: Processed files in {args.path}")
        if total_issues > 0:
            print(f"✅ Fixed {total_issues} issues across {files_with_issues} files")
        else:
            print("✅ No issues found to fix")
    else:
        print(f"\n📊 Validation Summary:")
        print(f"   Files with issues: {files_with_issues}")
        print(f"   Total issues found: {total_issues}")

        if total_issues > 0:
            print(f"\n💡 Run with --fix to automatically resolve these issues")
            sys.exit(1)
        else:
            print("✅ All markdown files are properly formatted!")

if __name__ == "__main__":
    main()
