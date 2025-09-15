# Publishing to PyPI

This document describes how to publish dfdrift to PyPI.

## Prerequisites

1. Create accounts on:
   - [Test PyPI](https://test.pypi.org/account/register/) (for testing)
   - [PyPI](https://pypi.org/account/register/) (for production)

2. Generate API tokens:
   - [Test PyPI tokens](https://test.pypi.org/manage/account/token/)
   - [PyPI tokens](https://pypi.org/manage/account/token/)

## Local Publishing Steps

### 1. Build the package

```bash
# Clean previous builds
rm -rf dist/ build/ *.egg-info

# Build distribution files
uv build
```

This creates:
- `dist/dfdrift-0.1.0.tar.gz` (source distribution)
- `dist/dfdrift-0.1.0-py3-none-any.whl` (wheel distribution)

### 2. Test on Test PyPI

```bash
# Upload to Test PyPI
uv publish --publish-url https://test.pypi.org/legacy/ --username __token__ --password <test-pypi-token>

# Test installation from Test PyPI
pip install --index-url https://test.pypi.org/simple/ dfdrift
```

### 3. Publish to Production PyPI

```bash
# Upload to production PyPI (uses default PyPI URL)
uv publish --username __token__ --password <pypi-token>

# Test installation from PyPI
pip install dfdrift
```

## Using Environment Variables

For automation, you can use environment variables:

```bash
# Set tokens as environment variables
export UV_PUBLISH_TOKEN=<your-token>

# Publish to Test PyPI
uv publish --publish-url https://test.pypi.org/legacy/

# Publish to production PyPI
uv publish
```

## Version Management

Before publishing:

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` (if exists)
3. Commit changes
4. Create git tag: `git tag v0.1.0 && git push origin v0.1.0`

## Troubleshooting

- **403 Forbidden**: Check your API token and permissions
- **409 Conflict**: Version already exists, increment version number
- **400 Bad Request**: Check package metadata in `pyproject.toml`