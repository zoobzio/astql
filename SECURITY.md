# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible for receiving such patches depends on the CVSS v3.0 Rating:

| Version | Supported          | Status |
| ------- | ------------------ | ------ |
| latest  | ✅ | Active development |
| < latest | ❌ | Security fixes only for critical issues |

## Reporting a Vulnerability

We take the security of ASTQL seriously. If you have discovered a security vulnerability in this project, please report it responsibly.

### How to Report

**Please DO NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via one of the following methods:

1. **GitHub Security Advisories** (Preferred)
   - Go to the [Security tab](https://github.com/zoobzio/astql/security) of this repository
   - Click "Report a vulnerability"
   - Fill out the form with details about the vulnerability

2. **Email**
   - Send details to the repository maintainer through GitHub profile contact information
   - Use PGP encryption if possible for sensitive details

### What to Include

Please include the following information (as much as you can provide) to help us better understand the nature and scope of the possible issue:

- **Type of issue** (e.g., SQL injection, query manipulation, parameter injection, etc.)
- **Full paths of source file(s)** related to the manifestation of the issue
- **The location of the affected source code** (tag/branch/commit or direct URL)
- **Any special configuration required** to reproduce the issue
- **Step-by-step instructions** to reproduce the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact of the issue**, including how an attacker might exploit the issue
- **Which database provider** (PostgreSQL, SQLite, or both) is affected
- **Your name and affiliation** (optional)

### Security Considerations for ASTQL

Given that ASTQL is a SQL query builder, please pay special attention to:

- **SQL Injection vulnerabilities** - Any way to bypass parameterization
- **Parameter validation issues** - Improper sanitization of parameter names
- **Field/Table validation bypasses** - Ways to inject arbitrary SQL through field or table names
- **Schema validation vulnerabilities** - Issues with YAML/JSON schema parsing
- **Provider-specific vulnerabilities** - Issues unique to PostgreSQL or SQLite implementations

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Initial Assessment**: Within 7 days, we will provide an initial assessment of the report
- **Resolution Timeline**: We aim to resolve critical issues within 30 days
- **Disclosure**: We will coordinate with you on the disclosure timeline
- **Credit**: Security researchers who responsibly disclose vulnerabilities will be acknowledged (unless they prefer to remain anonymous)

## Security Best Practices for Users

When using ASTQL in your applications:

1. **Always use parameterized queries** - Never concatenate user input directly
2. **Validate all input** - Use the `Try*` functions for user-provided data
3. **Use sentinel for struct validation** - Register all structs with sentinel before use
4. **Keep dependencies updated** - Regularly update ASTQL and its dependencies
5. **Review generated SQL** - In development, log and review the generated SQL queries
6. **Use least privilege** - Database connections should have minimal required permissions
7. **Sanitize schema inputs** - When using schema-based queries, validate all YAML/JSON input

## Security Features

ASTQL includes several security features:

- **Automatic parameterization** - All values are parameterized by default
- **Input validation** - Field, table, and parameter names are validated
- **SQL keyword blocking** - Parameter names cannot be SQL keywords
- **Injection prevention** - Special characters in identifiers are escaped
- **Type safety** - Go's type system prevents many common mistakes
- **Provider isolation** - Database-specific code is isolated per provider

## Contact

For security-related questions that are not vulnerabilities, please open a discussion in the repository.

Thank you for helping keep ASTQL and its users safe!