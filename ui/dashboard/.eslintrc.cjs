module.exports = {
  root: true,
  extends: [
    'standard', // Use the StandardJS style rules
    'plugin:svelte/recommended',
    'prettier',
  ],
  plugins: ['standard'], // Enable the StandardJS plugin
  parserOptions: {
    sourceType: 'module',
    ecmaVersion: 2020,
    extraFileExtensions: ['.svelte'],
  },
  env: {
    browser: true,
    es2017: true,
    node: true,
  },
  rules: {
    // Configure any specific rules or overrides here
    semi: 'off', // Turn off semicolon requirement
  },
}
