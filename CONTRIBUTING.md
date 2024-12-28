# Contributing to pilgrimage

Thank you for your interest in contributing to **[Project Name]**! We appreciate your time and effort in improving this project. This guide will help you get started.

---

## Table of Contents
1. [Getting Started](#getting-started)
2. [How to Report Issues](#how-to-report-issues)
3. [Submitting Changes](#submitting-changes)
4. [Code of Conduct](#code-of-conduct)
5. [Style Guide](#style-guide)
6. [License](#license)

---

## Getting Started

1. **Fork the repository**:
   - Click the "Fork" button at the top-right corner of this repository.
   - Clone your fork locally:
     ```bash
     git clone https://github.com/your-username/[project-name].git
     ```

2. **Set up the project**:
   - Follow the instructions in the `README.md` to install dependencies and run the project locally.

3. **Create a branch**:
   - Always create a new branch for your work:
     ```bash
     git checkout -b feature/your-feature-name
     ```

---

## How to Report Issues

1. **Search for existing issues**:
   - Before opening a new issue, check the [Issues](https://github.com/[organization-name]/[project-name]/issues) tab to see if it has already been reported.

2. **Create a new issue**:
   - If no existing issue matches, open a new one. Include:
     - A clear and descriptive title.
     - Steps to reproduce (if applicable).
     - Expected vs. actual results.
     - Screenshots or error logs, if possible.

3. **Label the issue**:
   - Use relevant labels such as `bug`, `enhancement`, or `documentation`.

---

## Submitting Changes

1. **Write clear, concise commits**:
   - Follow this format for commit messages:
     ```
     feat: Short description of your feature
     fix: Short description of the bug fixed
     docs: Update documentation
     ```

2. **Push to your branch**:
   ```bash
   git push origin feature/your-feature-name

3. **Open a pull request:**
  - Go to your fork on GitHub and click "Compare & Pull Request".
  - Include:
    - A clear and descriptive title.
    - A detailed description of your changes.
  - References to related issues (e.g., "Closes #123").

4. **Wait for review:**
  - Address feedback from maintainers promptly.

## Style Guide

1. Follow project conventions:
  - Code must adhere to existing styles and patterns in the project.

2. Linting and formatting:
  - Run linting tools before committing:
  ```sh
  cargo fmt && cargo clippy
  ```

3. Write tests:
- Ensure new features or fixes are accompanied by appropriate tests.
