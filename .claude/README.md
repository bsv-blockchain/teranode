# Claude Agents and Commands for Teranode

This directory contains specialized Claude agents and commands tailored for the Teranode Go/microservices project.

## Agents

### Testing Agents

#### `api-tester.md`
Comprehensive API testing specialist for performance, load testing, and contract validation.
- **Best for**: Load testing, API performance analysis, contract testing
- **Tools**: k6, JMeter, Postman, cURL
- **Key features**: Load simulation, bottleneck identification, security testing

#### `performance-benchmarker.md`
Performance optimization expert for speed testing and bottleneck identification.
- **Best for**: Performance profiling, speed optimization, resource analysis
- **Tools**: Profiling tools, benchmarking utilities
- **Key features**: CPU/Memory analysis, query optimization, performance budgets

#### `test-writer-fixer.md`
Test automation expert for writing comprehensive tests and maintaining test suite integrity.
- **Best for**: Writing unit/integration tests, fixing failing tests, TDD
- **Languages**: Go (testify, gomega), JavaScript, Python
- **Key features**: Test execution strategy, failure analysis, quality assurance

#### `test-results-analyzer.md`
Test data analysis expert for synthesizing results and generating quality metrics.
- **Best for**: Quality reporting, trend analysis, flaky test detection
- **Key features**: Test health metrics, coverage analysis, executive dashboards

### Engineering Agents

#### `backend-architect.md`
Master backend architect for scalable, secure server-side systems.
- **Best for**: API design, database architecture, system design
- **Languages**: Go, Node.js, Python, Java
- **Key features**: Microservices, security implementation, performance optimization

#### `devops-automator.md`
DevOps automation expert for CI/CD, infrastructure, and monitoring.
- **Best for**: Pipeline setup, infrastructure automation, monitoring
- **Tools**: GitHub Actions, Terraform, Docker, Kubernetes
- **Key features**: Zero-downtime deployments, auto-scaling, observability

## Commands

### Issue Management
- **`/start-issue <number>`** - Begin working on a GitHub issue
- **`/fix-issue <number>`** - Analyze and fix a GitHub issue
- **`/finish-issue <number>`** - Complete work on a GitHub issue

### Development Workflow
- **`/publish`** - Create conventional commit and push changes
- **`/simplify`** - Analyze and consolidate recent changes

## Usage Examples

### Testing Workflow
```bash
# After making API changes
@api-tester "Test the new authentication endpoints under load"

# For comprehensive test coverage
@test-writer-fixer "Add tests for the new block validation logic"

# To analyze test trends
@test-results-analyzer "Generate quality report for this sprint"
```

### Architecture and DevOps
```bash
# For system design decisions
@backend-architect "Design a scalable API for transaction processing"

# For deployment improvements
@devops-automator "Set up monitoring and alerting for the new service"
```

### Issue Management
```bash
# Start working on an issue
/start-issue 123

# Fix a specific issue
/fix-issue 456

# Complete issue work
/finish-issue 123
```

## Go-Specific Optimizations

These agents are optimized for Go development with:

- **Testing**: Support for Go's testing package, testify, and gomega
- **Performance**: Go-specific profiling tools (pprof, benchmarks)
- **Architecture**: Microservices patterns common in Go projects
- **DevOps**: Container optimization for Go applications

## Best Practices

1. **Use agents proactively**: Invoke test-writer-fixer after code changes
2. **Combine agents**: Use api-tester with performance-benchmarker for comprehensive analysis
3. **Follow conventions**: All commands respect project's conventional commit standards
4. **Iterative improvement**: Use test-results-analyzer for continuous quality insights

## Project-Specific Notes

- All agents understand this is a Bitcoin/blockchain project
- Security considerations are emphasized throughout
- Performance is critical for blockchain applications
- Agents are configured to avoid AI attribution in commits per project standards