# Teranode UI v2

## Index

1. [Docs](docs/readme.md)
2. [Todos](#todos)
3. [Svelte project notes](#svelte-project-notes)

## Project structure

Besides the special `lib` folder that comes with SvelteKit projects (that allows imports via `$lib/*`), another one called `internal` is defined in [svelte.config.js](./svelte.config.js)

The intention behind each folder is the following

- `lib` : code in this folder is supposed to end up in a shared library of some sort in future. Therefore, consider code going here to be wider than project-scope and that the code may be split out from this repository.
- `internal` : code in this folder is very much project-specific and unsuitable for a shared library. It could be that new UI components, for instance, start their life in `internal`, but after taking on a more generic nature get moved to `$lib/components`.

## Todos

See [./todos.md](./todos.md)

## Svelte project notes

See [./svelte.md](./svelte.md)
