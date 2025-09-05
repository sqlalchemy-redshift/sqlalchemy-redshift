# Security Policy

## Supported Versions

We provide security updates for the following versions of sqlalchemy-redshift:

| Version | Supported          |
| ------- | ------------------ |
| 0.8.x   | ✅ |
| < 0.8   | ❌ |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security vulnerability in sqlalchemy-redshift, please report it responsibly.

### How to Report

1. **DO NOT** create a public GitHub issue for security vulnerabilities
2. Email security details to the maintainers through GitHub's private vulnerability reporting feature
3. Or create a private security advisory through GitHub's interface

### What to Include

When reporting a vulnerability, please include:

- A clear description of the vulnerability
- Steps to reproduce the issue
- Potential impact and affected versions
- Any suggested fixes or mitigation strategies
- Your contact information for follow-up questions

### Response Timeline

- **Acknowledgment**: We will acknowledge receipt within 2 business days
- **Initial Response**: We will provide an initial response within 5 business days
- **Fix Timeline**: We aim to release fixes for high-severity issues within 30 days

### Disclosure Policy

- We will work with you to understand and resolve the issue
- We will coordinate disclosure timing to ensure users have time to update
- We will credit you in our security advisory (unless you prefer to remain anonymous)

### Security Best Practices

When using sqlalchemy-redshift:

1. **Keep Dependencies Updated**: Regularly update to the latest version
2. **Secure Connections**: Always use SSL/TLS for database connections
3. **Credential Management**: Never hardcode credentials in your code
4. **Input Validation**: Always validate and sanitize user inputs
5. **Least Privilege**: Use database users with minimal required permissions

## Security Features

sqlalchemy-redshift includes several security features:

- SSL/TLS connection support by default
- Parameterized queries to prevent SQL injection
- Support for IAM role-based authentication with AWS Redshift

Thank you for helping keep sqlalchemy-redshift secure!