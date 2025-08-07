# Commit and Push Changes

Create a conventional commit with all current changes and push to the remote repository.

## Steps

1. Run `git status` to see all changes
2. Run `git diff` to review the changes
3. Analyze the changes and determine the appropriate conventional commit type:

   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `style:` for formatting changes
   - `refactor:` for code refactoring
   - `test:` for test additions/changes
   - `chore:` for maintenance tasks

4. Stage all changes with `git add -A`
5. Create a conventional commit with a descriptive message
6. Push to the current branch with `git push`

Remember: This project follows Conventional Commits specification. Do NOT add co-authors to the commit message.